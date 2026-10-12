package dao_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/test"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func waitJobCompleted(t *testing.T, db *gorm.DB, jobID uint, timeout time.Duration) entity.PageFetchJob {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var job entity.PageFetchJob
		if err := db.First(&job, jobID).Error; err != nil {
			t.Fatalf("load job %d: %v", jobID, err)
		}
		if job.Status == entity.PageFetchJobStatusCompleted {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %d did not complete within %s", jobID, timeout)
	return entity.PageFetchJob{}
}

func waitItemStatus(t *testing.T, db *gorm.DB, jobID, pageID uint, status string, timeout time.Duration) entity.PageFetchJobItem {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var item entity.PageFetchJobItem
		if err := db.Where("job_id = ? AND page_id = ?", jobID, pageID).First(&item).Error; err != nil {
			t.Fatalf("load item (job %d, page %d): %v", jobID, pageID, err)
		}
		if item.Status == status {
			return item
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("item (job %d, page %d) did not reach status %q within %s", jobID, pageID, status, timeout)
	return entity.PageFetchJobItem{}
}

func TestPageFetchJob(t *testing.T) {
	app, err := test.NewTestApp()
	assert.NoError(t, err)
	defer app.Cleanup()
	db := app.Dao().DB()

	var (
		okHits     atomic.Int32
		rSuccHits  atomic.Int32
		flakyCount atomic.Int32
	)

	block := func(title string) (chan struct{}, http.HandlerFunc) {
		ch := make(chan struct{})
		return ch, func(w http.ResponseWriter, r *http.Request) {
			<-ch
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, `<html><head><title>%s</title></head></html>`, title)
		}
	}

	g1Ch, hG1 := block("Fetched G1")
	g2Ch, hG2 := block("Fetched G2")
	g3Ch, hG3 := block("Fetched G3")
	bBlockCh, hBBlock := block("B Block Title")
	rBlockCh, hRBlock := block("Resume Block Title")

	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) {
		okHits.Add(1)
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><head><title>OK Title</title></head></html>`)
	})
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ok", http.StatusFound)
	})
	mux.HandleFunc("/meta", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `<html><head><meta http-equiv="refresh" content="0; url=http://%s/ok"></head></html>`, r.Host)
	})
	mux.HandleFunc("/fail", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/flaky", func(w http.ResponseWriter, r *http.Request) {
		if flakyCount.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, `<html><head><title>Flaky Title</title></head></html>`)
	})
	mux.HandleFunc("/notitle", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><body>no title here</body></html>`)
	})
	mux.HandleFunc("/g1", hG1)
	mux.HandleFunc("/g2", hG2)
	mux.HandleFunc("/g3", hG3)
	mux.HandleFunc("/bblock", hBBlock)
	mux.HandleFunc("/rblock", hRBlock)
	mux.HandleFunc("/resume-ok", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<html><head><title>Resume OK</title></head></html>`)
	})
	mux.HandleFunc("/r-success", func(w http.ResponseWriter, r *http.Request) {
		rSuccHits.Add(1)
		fmt.Fprint(w, `<html><head><title>Already Done</title></head></html>`)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	siteA := "PFJ_A_" + suffix
	siteB := "PFJ_B_" + suffix
	assert.NoError(t, db.Create(&entity.Site{Name: siteA, Urls: ""}).Error) // no site URL -> relative keys invalid
	assert.NoError(t, db.Create(&entity.Site{Name: siteB, Urls: srv.URL}).Error)

	createPage := func(site, key, title string) entity.Page {
		p := entity.Page{Key: key, Title: title, SiteName: site}
		assert.NoError(t, db.Create(&p).Error)
		return p
	}
	urlPage := func(path string) string { return srv.URL + path }

	pOK := createPage(siteA, urlPage("/ok"), "Old OK")
	pRedir := createPage(siteA, urlPage("/redirect"), "Old Redir")
	pMeta := createPage(siteA, urlPage("/meta"), "Old Meta")
	pFail := createPage(siteA, urlPage("/fail"), "Old Fail")
	pFlaky := createPage(siteA, urlPage("/flaky"), "Old Flaky")
	pNoTitle := createPage(siteA, urlPage("/notitle"), "Keep NoTitle")
	pInvalid := createPage(siteA, "/relative/no-site-url.html", "Keep Invalid")

	// 1. Normal batch: per-page fetch / redirect / failure results
	t.Run("normal batch with per-page results", func(t *testing.T) {
		job, err := app.Dao().StartPageFetchJob(siteA)
		assert.NoError(t, err)
		job = ptrJob(waitJobCompleted(t, db, job.ID, 20*time.Second))

		assert.Equal(t, 7, job.Total)
		assert.Equal(t, 4, job.Success)
		assert.Equal(t, 3, job.Failed)
		assert.Equal(t, 0, job.Skipped)

		// titles published
		assert.Equal(t, "OK Title", app.Dao().FindPageByID(pOK.ID).Title)
		assert.Equal(t, "Keep NoTitle", app.Dao().FindPageByID(pNoTitle.ID).Title) // empty title keeps old value

		job, items, err := app.Dao().FindPageFetchJob(job.ID)
		assert.NoError(t, err)

		itemByPage := map[uint]entity.PageFetchJobItem{}
		for _, it := range items {
			itemByPage[it.PageID] = it
		}

		assert.Equal(t, entity.PageFetchItemStatusSuccess, itemByPage[pOK.ID].Status)
		assert.NotNil(t, itemByPage[pOK.ID].PublishedAt)

		redir := itemByPage[pRedir.ID]
		assert.Equal(t, entity.PageFetchItemStatusSuccess, redir.Status)
		assert.True(t, redir.Redirected, "HTTP redirect should be recorded")
		assert.Contains(t, redir.FinalURL, "/ok")

		meta := itemByPage[pMeta.ID]
		assert.Equal(t, entity.PageFetchItemStatusSuccess, meta.Status)
		assert.True(t, meta.Redirected, "meta refresh redirect should be recorded")
		assert.Contains(t, meta.FinalURL, "/ok")

		assert.Equal(t, entity.PageFetchItemStatusSuccess, itemByPage[pNoTitle.ID].Status)
		assert.Equal(t, entity.PageFetchItemStatusFailed, itemByPage[pFail.ID].Status)
		assert.NotEmpty(t, itemByPage[pFail.ID].Error)
		assert.Equal(t, 1, itemByPage[pFail.ID].Attempts)
		assert.Equal(t, entity.PageFetchItemStatusFailed, itemByPage[pFlaky.ID].Status)
		assert.Equal(t, entity.PageFetchItemStatusFailed, itemByPage[pInvalid.ID].Status)
		assert.Contains(t, itemByPage[pInvalid.ID].Error, "no valid URL")
	})

	// 2. Retry only failed items; succeeded pages are never re-fetched
	t.Run("retry failed items only", func(t *testing.T) {
		var lastJob entity.PageFetchJob
		assert.NoError(t, db.Where("site_name = ?", siteA).Order("id DESC").First(&lastJob).Error)

		hitsBefore := okHits.Load()
		job, err := app.Dao().RetryFailedPageFetchJob(lastJob.ID)
		assert.NoError(t, err)
		job = ptrJob(waitJobCompleted(t, db, job.ID, 20*time.Second))

		assert.Equal(t, 5, job.Success)
		assert.Equal(t, 2, job.Failed) // /fail still broken, invalid URL still invalid
		assert.Equal(t, 0, job.Skipped)
		assert.Equal(t, hitsBefore, okHits.Load(), "succeeded pages must not be fetched again")

		assert.Equal(t, "Flaky Title", app.Dao().FindPageByID(pFlaky.ID).Title)

		_, items, err := app.Dao().FindPageFetchJob(job.ID)
		assert.NoError(t, err)
		for _, it := range items {
			switch it.PageID {
			case pFlaky.ID:
				assert.Equal(t, entity.PageFetchItemStatusSuccess, it.Status)
				assert.Equal(t, 2, it.Attempts)
			case pFail.ID, pInvalid.ID:
				assert.Equal(t, entity.PageFetchItemStatusFailed, it.Status)
				assert.Equal(t, 2, it.Attempts)
			case pOK.ID:
				assert.Equal(t, 1, it.Attempts)
			}
		}
	})

	// 3. Late results are skipped when the page was renamed / deleted / title manually changed
	t.Run("stale results are skipped not restored", func(t *testing.T) {
		pGTitle := createPage(siteA, urlPage("/g1"), "Old G1")
		pGKey := createPage(siteA, urlPage("/g2"), "Old G2")
		pGDel := createPage(siteA, urlPage("/g3"), "Old G3")

		job, err := app.Dao().StartPageFetchJob(siteA)
		assert.NoError(t, err)

		// runner is sequential; wait until it is holding the request of g1
		waitItemStatus(t, db, job.ID, pGTitle.ID, entity.PageFetchItemStatusProcessing, 20*time.Second)

		// admin changes while fetches are in flight
		assert.NoError(t, db.Model(&entity.Page{}).Where("id = ?", pGTitle.ID).
			Updates(map[string]any{"title": "Manual Title", "title_version": gorm.Expr("title_version + 1")}).Error)
		assert.NoError(t, db.Model(&entity.Page{}).Where("id = ?", pGKey.ID).
			Update("key", "/renamed/g2.html").Error)
		assert.NoError(t, db.Delete(&entity.Page{}, pGDel.ID).Error)

		close(g1Ch)
		close(g2Ch)
		close(g3Ch)
		job = ptrJob(waitJobCompleted(t, db, job.ID, 20*time.Second))

		_, items, err := app.Dao().FindPageFetchJob(job.ID)
		assert.NoError(t, err)
		byPage := map[uint]entity.PageFetchJobItem{}
		for _, it := range items {
			byPage[it.PageID] = it
		}

		assert.Equal(t, entity.PageFetchItemStatusSkipped, byPage[pGTitle.ID].Status)
		assert.Equal(t, entity.PageFetchSkipReasonTitleChanged, byPage[pGTitle.ID].SkipReason)
		assert.Equal(t, entity.PageFetchItemStatusSkipped, byPage[pGKey.ID].Status)
		assert.Equal(t, entity.PageFetchSkipReasonKeyChanged, byPage[pGKey.ID].SkipReason)
		assert.Equal(t, entity.PageFetchItemStatusSkipped, byPage[pGDel.ID].Status)
		assert.Equal(t, entity.PageFetchSkipReasonDeleted, byPage[pGDel.ID].SkipReason)

		assert.Equal(t, 3, job.Skipped)

		// manual title must survive; renamed page must not be overwritten; deleted page must not be recreated
		assert.Equal(t, "Manual Title", app.Dao().FindPageByID(pGTitle.ID).Title)
		assert.Equal(t, 1, app.Dao().FindPageByID(pGTitle.ID).TitleVersion)

		renamed := app.Dao().FindPage("/renamed/g2.html", siteA)
		assert.False(t, renamed.IsEmpty())
		assert.Equal(t, "Old G2", renamed.Title)
		assert.True(t, app.Dao().FindPageByID(pGDel.ID).IsEmpty())

		// soft-deleted row stays deleted, not resurrected by the late result
		var deleted entity.Page
		err = db.Unscoped().First(&deleted, pGDel.ID).Error
		assert.NoError(t, err)
		assert.True(t, deleted.DeletedAt.Valid)
		assert.Equal(t, "Old G3", deleted.Title)
	})

	// 4. Same scope returns the running job; different scopes never block each other
	t.Run("concurrent same-scope start and independent scopes", func(t *testing.T) {
		pBBlock := createPage(siteB, urlPage("/bblock"), "Old B")

		jobB, err := app.Dao().StartPageFetchJob(siteB)
		assert.NoError(t, err)
		waitItemStatus(t, db, jobB.ID, pBBlock.ID, entity.PageFetchItemStatusProcessing, 20*time.Second)

		var wg sync.WaitGroup
		ids := make(chan uint, 16)
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				j, e := app.Dao().StartPageFetchJob(siteB)
				assert.NoError(t, e)
				ids <- j.ID
			}()
		}
		wg.Wait()
		close(ids)
		for id := range ids {
			assert.Equal(t, jobB.ID, id, "same scope must return the running job")
		}

		// site A job runs to completion while site B is still blocked
		jobA, err := app.Dao().StartPageFetchJob(siteA)
		assert.NoError(t, err)
		jobA = ptrJob(waitJobCompleted(t, db, jobA.ID, 20*time.Second))

		var stillRunning entity.PageFetchJob
		assert.NoError(t, db.First(&stillRunning, jobB.ID).Error)
		assert.Equal(t, entity.PageFetchJobStatusRunning, stillRunning.Status)

		close(bBlockCh)
		jobB = ptrJob(waitJobCompleted(t, db, jobB.ID, 20*time.Second))
		assert.Equal(t, 1, jobB.Total)
		assert.Equal(t, 1, jobB.Success)
		assert.Equal(t, "B Block Title", app.Dao().FindPageByID(pBBlock.ID).Title)
	})

	// 5. Simulated restart: success items are not re-fetched, interrupted ones resume
	t.Run("resume interrupted job after restart", func(t *testing.T) {
		pROk := createPage(siteB, urlPage("/resume-ok"), "Old ROk")
		pRBlock := createPage(siteB, urlPage("/rblock"), "Old RBlock")
		pRSucc := createPage(siteB, urlPage("/r-success"), "Old RSucc")

		now := time.Now()
		job := entity.PageFetchJob{
			SiteName: siteB,
			Status:   entity.PageFetchJobStatusRunning,
			Total:    3,
			Success:  1,
		}
		assert.NoError(t, db.Create(&job).Error)

		// lower id first: pending page is claimable immediately,
		// the "processing" one represents a page whose result was never confirmed
		items := []entity.PageFetchJobItem{
			{JobID: job.ID, PageID: pROk.ID, PageKey: pROk.Key, SiteName: siteB, Status: entity.PageFetchItemStatusPending},
			{JobID: job.ID, PageID: pRBlock.ID, PageKey: pRBlock.Key, SiteName: siteB, Status: entity.PageFetchItemStatusProcessing, Attempts: 1},
			{JobID: job.ID, PageID: pRSucc.ID, PageKey: pRSucc.Key, SiteName: siteB, Status: entity.PageFetchItemStatusSuccess, Attempts: 3, PublishedAt: &now},
		}
		assert.NoError(t, db.CreateInBatches(&items, 10).Error)

		app.Dao().ResumePageFetchJobs()

		assert.Equal(t, entity.PageFetchItemStatusSuccess,
			waitItemStatus(t, db, job.ID, pROk.ID, entity.PageFetchItemStatusSuccess, 20*time.Second).Status)
		assert.Equal(t, "Resume OK", app.Dao().FindPageByID(pROk.ID).Title)

		blocked := waitItemStatus(t, db, job.ID, pRBlock.ID, entity.PageFetchItemStatusProcessing, 20*time.Second)
		assert.Equal(t, 2, blocked.Attempts, "unconfirmed processing item should be retried once after restart")

		// already confirmed success item must not be fetched again
		assert.Equal(t, int32(0), rSuccHits.Load())
		var succItem entity.PageFetchJobItem
		assert.NoError(t, db.Where("job_id = ? AND page_id = ?", job.ID, pRSucc.ID).First(&succItem).Error)
		assert.Equal(t, entity.PageFetchItemStatusSuccess, succItem.Status)
		assert.Equal(t, 3, succItem.Attempts)

		close(rBlockCh)
		done := waitJobCompleted(t, db, job.ID, 20*time.Second)
		assert.Equal(t, 3, done.Success)
		assert.Equal(t, 0, done.Failed)
		assert.Equal(t, 0, done.Skipped)
	})

	// 6. Empty site: a completed job with zero pages
	t.Run("empty site", func(t *testing.T) {
		emptySite := "PFJ_Empty_" + suffix
		assert.NoError(t, db.Create(&entity.Site{Name: emptySite, Urls: ""}).Error)

		job, err := app.Dao().StartPageFetchJob(emptySite)
		assert.NoError(t, err)
		job = ptrJob(waitJobCompleted(t, db, job.ID, 10*time.Second))
		assert.Equal(t, 0, job.Total)
		assert.Equal(t, 0, job.Success)
		assert.Equal(t, 0, job.Failed)
		assert.Equal(t, 0, job.Skipped)
	})

	// 7. Single-page refresh behavior is unchanged: invalid URL keeps old title and returns nil
	t.Run("single page fetch keeps old behavior", func(t *testing.T) {
		invalid := entity.Page{Key: "/relative/again.html", Title: "Keep Me", SiteName: siteA}
		assert.NoError(t, app.Dao().FetchPageFromURL(&invalid))
		assert.Equal(t, "Keep Me", invalid.Title)

		ok := createPage(siteA, urlPage("/ok"), "Before Single")
		assert.NoError(t, app.Dao().FetchPageFromURL(&ok))
		assert.Equal(t, "OK Title", ok.Title)
	})
}

func ptrJob(j entity.PageFetchJob) *entity.PageFetchJob { return &j }
