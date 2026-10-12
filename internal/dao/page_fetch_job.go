package dao

import (
	"errors"
	"sync"
	"time"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/log"
	"github.com/artalkjs/artalk/v2/internal/utils"
	"gorm.io/gorm"
)

var ErrPageFetchJobNotFound = errors.New("page fetch job not found")

// Serializes job start/retry within the same scope ("" means all sites).
// Different scopes use different locks and never block each other.
func (dao *Dao) pageFetchScopeLock(scope string) *sync.Mutex {
	m, _ := dao.pageFetchScopeLocks.LoadOrStore(scope, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// StartPageFetchJob creates and starts a batch page-fetch job for the given
// site scope (empty siteName means all sites).
//
// The scope and the per-page key/site/title-version are snapshotted at start.
// If a running job already exists for the scope, that job is returned instead
// of creating a new one.
func (dao *Dao) StartPageFetchJob(siteName string) (*entity.PageFetchJob, error) {
	mu := dao.pageFetchScopeLock(siteName)
	mu.Lock()
	defer mu.Unlock()

	var existing entity.PageFetchJob
	err := dao.DB().Where("site_name = ? AND status = ?", siteName, entity.PageFetchJobStatusRunning).
		Order("id DESC").First(&existing).Error
	if err == nil {
		return &existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// Snapshot the pages in scope
	var pages []entity.Page
	query := dao.DB().Model(&entity.Page{}).Order("id ASC")
	if siteName != "" {
		query = query.Where("site_name = ?", siteName)
	}
	if err := query.Find(&pages).Error; err != nil {
		return nil, err
	}

	job := &entity.PageFetchJob{
		SiteName: siteName,
		Status:   entity.PageFetchJobStatusRunning,
		Total:    len(pages),
	}
	items := make([]entity.PageFetchJobItem, 0, len(pages))
	for _, p := range pages {
		items = append(items, entity.PageFetchJobItem{
			PageID:       p.ID,
			PageKey:      p.Key,
			SiteName:     p.SiteName,
			TitleVersion: p.TitleVersion,
			Status:       entity.PageFetchItemStatusPending,
		})
	}

	if err := dao.DB().Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(job).Error; err != nil {
			return err
		}
		for i := range items {
			items[i].JobID = job.ID
		}
		if len(items) > 0 {
			if err := tx.CreateInBatches(&items, 500).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	dao.runPageFetchJobAsync(job.ID)
	return job, nil
}

// RetryFailedPageFetchJob resets only the failed items of a finished job and
// resumes it. A running job is returned unchanged.
func (dao *Dao) RetryFailedPageFetchJob(jobID uint) (*entity.PageFetchJob, error) {
	var job entity.PageFetchJob
	if err := dao.DB().First(&job, jobID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrPageFetchJobNotFound
		}
		return nil, err
	}

	mu := dao.pageFetchScopeLock(job.SiteName)
	mu.Lock()
	defer mu.Unlock()

	if job.Status == entity.PageFetchJobStatusRunning {
		return &job, nil
	}

	if err := dao.DB().Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&entity.PageFetchJobItem{}).
			Where("job_id = ? AND status = ?", jobID, entity.PageFetchItemStatusFailed).
			Update("status", entity.PageFetchItemStatusPending).Error; err != nil {
			return err
		}
		if err := tx.Model(&entity.PageFetchJob{}).Where("id = ?", jobID).
			Update("status", entity.PageFetchJobStatusRunning).Error; err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	job.Status = entity.PageFetchJobStatusRunning
	if err := dao.refreshPageFetchJobCounts(jobID, map[string]any{}); err != nil {
		return nil, err
	}
	dao.runPageFetchJobAsync(jobID)
	return &job, nil
}

// FindPageFetchJob loads a job together with all of its items.
func (dao *Dao) FindPageFetchJob(jobID uint) (*entity.PageFetchJob, []entity.PageFetchJobItem, error) {
	var job entity.PageFetchJob
	if err := dao.DB().First(&job, jobID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, ErrPageFetchJobNotFound
		}
		return nil, nil, err
	}

	var items []entity.PageFetchJobItem
	if err := dao.DB().Where("job_id = ?", jobID).Order("id ASC").Find(&items).Error; err != nil {
		return nil, nil, err
	}
	return &job, items, nil
}

// FindLatestPageFetchJob returns the most recent job for the given scope.
func (dao *Dao) FindLatestPageFetchJob(siteName string) (*entity.PageFetchJob, error) {
	var job entity.PageFetchJob
	err := dao.DB().Where("site_name = ?", siteName).Order("id DESC").First(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &job, nil
}

// ResumePageFetchJobs continues all jobs left in "running" state by a previous
// process. Only unconfirmed items (pending / processing) are retried;
// success / skipped / failed items are never re-fetched.
func (dao *Dao) ResumePageFetchJobs() {
	var jobs []entity.PageFetchJob
	if err := dao.DB().Where("status = ?", entity.PageFetchJobStatusRunning).Find(&jobs).Error; err != nil {
		log.Error("[page_fetch_job] failed to resume jobs: ", err)
		return
	}

	for i := range jobs {
		// A "processing" item means the previous process died (or was restarted)
		// before the result was confirmed, so it goes back to the queue.
		if err := dao.DB().Model(&entity.PageFetchJobItem{}).
			Where("job_id = ? AND status = ?", jobs[i].ID, entity.PageFetchItemStatusProcessing).
			Update("status", entity.PageFetchItemStatusPending).Error; err != nil {
			log.Error("[page_fetch_job] failed to reset processing items: ", err)
		}
		if err := dao.refreshPageFetchJobCounts(jobs[i].ID, map[string]any{}); err != nil {
			log.Error("[page_fetch_job] failed to refresh counts: ", err)
		}
		dao.runPageFetchJobAsync(jobs[i].ID)
	}
}

func (dao *Dao) runPageFetchJobAsync(jobID uint) {
	if _, loaded := dao.pageFetchJobRunners.LoadOrStore(jobID, struct{}{}); loaded {
		return
	}
	go func() {
		defer dao.pageFetchJobRunners.Delete(jobID)
		dao.runPageFetchJob(jobID)
	}()
}

func (dao *Dao) runPageFetchJob(jobID uint) {
	for {
		var item entity.PageFetchJobItem
		err := dao.DB().
			Where("job_id = ? AND status = ?", jobID, entity.PageFetchItemStatusPending).
			Order("id ASC").First(&item).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			dao.finishPageFetchJob(jobID)
			return
		}
		if err != nil {
			log.Error("[page_fetch_job] claim next item error: ", err)
			time.Sleep(time.Second)
			continue
		}

		// Atomically claim the item, so concurrent runners never share it
		result := dao.DB().Model(&entity.PageFetchJobItem{}).
			Where("id = ? AND status = ?", item.ID, entity.PageFetchItemStatusPending).
			Updates(map[string]any{
				"status":   entity.PageFetchItemStatusProcessing,
				"attempts": gorm.Expr("attempts + 1"),
			})
		if result.Error != nil {
			log.Error("[page_fetch_job] claim item error: ", result.Error)
			continue
		}
		if result.RowsAffected == 0 {
			continue
		}

		item.Status = entity.PageFetchItemStatusProcessing
		item.Attempts++
		dao.processPageFetchItem(&item)
	}
}

func (dao *Dao) finishPageFetchJob(jobID uint) {
	counts := map[string]any{}
	if err := dao.refreshPageFetchJobCounts(jobID, counts); err != nil {
		log.Error("[page_fetch_job] count error: ", err)
		return
	}
	counts["status"] = entity.PageFetchJobStatusCompleted
	if err := dao.DB().Model(&entity.PageFetchJob{}).Where("id = ?", jobID).Updates(counts).Error; err != nil {
		log.Error("[page_fetch_job] finish error: ", err)
	}
}

func (dao *Dao) refreshPageFetchJobCounts(jobID uint, extra map[string]any) error {
	for status, column := range map[string]string{
		entity.PageFetchItemStatusSuccess: "success",
		entity.PageFetchItemStatusFailed:  "failed",
		entity.PageFetchItemStatusSkipped: "skipped",
	} {
		var count int64
		if err := dao.DB().Model(&entity.PageFetchJobItem{}).
			Where("job_id = ? AND status = ?", jobID, status).Count(&count).Error; err != nil {
			return err
		}
		extra[column] = int(count)
	}
	return dao.DB().Model(&entity.PageFetchJob{}).Where("id = ?", jobID).Updates(extra).Error
}

// processPageFetchItem fetches one page and publishes the result only if the
// page still belongs to the original site and its key / title version have not
// been touched by an admin since the job started.
func (dao *Dao) processPageFetchItem(item *entity.PageFetchJobItem) {
	// Build the fetch URL from the snapshotted identity
	snapPage := entity.Page{
		Model:    gorm.Model{ID: item.PageID},
		Key:      item.PageKey,
		SiteName: item.SiteName,
	}
	accessibleURL := dao.GetPageAccessibleURL(&snapPage)
	if accessibleURL == "" || !utils.ValidateURL(accessibleURL) {
		dao.failPageFetchItem(item, "no valid URL: "+accessibleURL)
		return
	}

	detail, err := fetchTitleDetailByURL(accessibleURL, 0)
	if err != nil {
		dao.failPageFetchItem(item, err.Error())
		return
	}

	// Guarded publish: verify the page identity and title version inside a
	// transaction and never create / restore deleted pages.
	var (
		page       entity.Page
		skipReason string
		published  bool
	)
	err = dao.DB().Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&page, item.PageID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				skipReason = entity.PageFetchSkipReasonDeleted
				return nil
			}
			return err
		}

		switch {
		case page.SiteName != item.SiteName:
			skipReason = entity.PageFetchSkipReasonSiteChanged
			return nil
		case page.Key != item.PageKey:
			skipReason = entity.PageFetchSkipReasonKeyChanged
			return nil
		case page.TitleVersion != item.TitleVersion:
			skipReason = entity.PageFetchSkipReasonTitleChanged
			return nil
		}

		if detail.Title != "" {
			page.Title = detail.Title
		}
		if err := tx.Save(&page).Error; err != nil {
			return err
		}
		published = true
		return nil
	})
	if err != nil {
		// Uncertain state: keep the item retryable, do not claim a result
		dao.failPageFetchItem(item, err.Error())
		return
	}

	if !published {
		item.Status = entity.PageFetchItemStatusSkipped
		item.SkipReason = skipReason
		item.Error = ""
		dao.savePageFetchItem(item)
		return
	}

	// Publish to cache only after the guarded DB commit succeeded
	dao.CacheAction(func(cache *DaoCache) {
		cache.PageCacheSave(&page)
	})

	now := time.Now()
	item.Status = entity.PageFetchItemStatusSuccess
	item.Title = detail.Title
	item.Redirected = detail.Redirected
	item.FinalURL = truncateJobStr(detail.FinalURL, 1024)
	item.SkipReason = ""
	item.Error = ""
	item.PublishedAt = &now
	dao.savePageFetchItem(item)
}

func (dao *Dao) failPageFetchItem(item *entity.PageFetchJobItem, reason string) {
	item.Status = entity.PageFetchItemStatusFailed
	item.Error = truncateJobStr(reason, 1024)
	dao.savePageFetchItem(item)
}

func (dao *Dao) savePageFetchItem(item *entity.PageFetchJobItem) {
	if err := dao.DB().Save(item).Error; err != nil {
		log.Error("[page_fetch_job] save item error: ", err)
	}
	if err := dao.refreshPageFetchJobCounts(item.JobID, map[string]any{}); err != nil {
		log.Error("[page_fetch_job] refresh counts error: ", err)
	}
}

func truncateJobStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
