package pagefetch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/artalkjs/artalk/v2/internal/dao"
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/pagefetch"
	"github.com/artalkjs/artalk/v2/test"
)

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// scriptFetcher records invocations, optionally gates a page fetch and runs a
// per-page script based on the current attempt.
type scriptFetcher struct {
	mu      sync.Mutex
	calls   map[uint]int
	script  map[uint]func(attempt int) (dao.PageTitleFetchResult, error)
	reached map[uint]chan struct{}
	gate    map[uint]chan struct{}
	signals sync.Map // pageID -> *sync.Once
}

func newScriptFetcher() *scriptFetcher {
	return &scriptFetcher{
		calls:   map[uint]int{},
		script:  map[uint]func(attempt int) (dao.PageTitleFetchResult, error){},
		reached: map[uint]chan struct{}{},
		gate:    map[uint]chan struct{}{},
	}
}

func (f *scriptFetcher) gatePage(pageID uint) (reached, release chan struct{}) {
	reached = make(chan struct{})
	release = make(chan struct{})
	f.mu.Lock()
	f.reached[pageID] = reached
	f.gate[pageID] = release
	f.mu.Unlock()
	return
}

func (f *scriptFetcher) scriptPage(pageID uint, fn func(attempt int) (dao.PageTitleFetchResult, error)) {
	f.mu.Lock()
	f.script[pageID] = fn
	f.mu.Unlock()
}

func (f *scriptFetcher) callCount(pageID uint) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[pageID]
}

func (f *scriptFetcher) Fetch(ctx context.Context, page *entity.Page) (dao.PageTitleFetchResult, error) {
	f.mu.Lock()
	f.calls[page.ID]++
	attempt := f.calls[page.ID]
	reached := f.reached[page.ID]
	gate := f.gate[page.ID]
	fn := f.script[page.ID]
	f.mu.Unlock()

	if reached != nil {
		if once, _ := f.signals.LoadOrStore(page.ID, &sync.Once{}); once != nil {
			once.(*sync.Once).Do(func() { close(reached) })
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return dao.PageTitleFetchResult{}, ctx.Err()
		}
	}

	if fn != nil {
		return fn(attempt)
	}
	return dao.PageTitleFetchResult{
		Title:     "fetched title",
		FinalURL:  page.Key,
		Redirects: 1,
	}, nil
}

func createTestPage(t *testing.T, d *dao.Dao, key, title, site string) entity.Page {
	t.Helper()
	p := d.NewPage(key, title, site)
	require.False(t, p.IsEmpty())
	return p
}

func TestManagerBatchPublishesAllPages(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()
	d := app.Dao()

	site := "PF Success Site"
	p1 := createTestPage(t, d, "https://example.com/a", "old A", site)
	p2 := createTestPage(t, d, "https://example.com/b", "old B", site)
	p3 := createTestPage(t, d, "https://example.com/c", "old C", site)

	f := newScriptFetcher()
	f.scriptPage(p1.ID, func(int) (dao.PageTitleFetchResult, error) {
		return dao.PageTitleFetchResult{Title: "new A", FinalURL: "https://example.com/a", Redirects: 2}, nil
	})
	f.scriptPage(p2.ID, func(int) (dao.PageTitleFetchResult, error) {
		return dao.PageTitleFetchResult{Title: "new B", FinalURL: "https://example.com/b", Redirects: 0}, nil
	})
	f.scriptPage(p3.ID, func(int) (dao.PageTitleFetchResult, error) {
		return dao.PageTitleFetchResult{Title: "new C", FinalURL: "https://example.com/c", Redirects: 0}, nil
	})

	m := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f.Fetch))
	defer m.Close()
	job, err := m.Start(site)
	require.NoError(t, err)

	waitUntil(t, 5*time.Second, func() bool {
		j, _, _ := m.Get(job.ID, false)
		return j.Status == entity.PageFetchJobCompleted
	})

	counts := m.Counts(job.ID)
	assert.Equal(t, 3, counts.Total)
	assert.Equal(t, 3, counts.Succeeded)
	assert.Zero(t, counts.Failed)
	assert.Zero(t, counts.Skipped)
	assert.Zero(t, counts.Remaining())

	_, items, err := m.Get(job.ID, true)
	require.NoError(t, err)
	redirects := map[uint]int{p1.ID: 2, p2.ID: 0, p3.ID: 0}
	for _, item := range items {
		assert.Equal(t, entity.PageFetchItemSucceeded, item.Status)
		assert.Equal(t, redirects[item.PageID], item.Redirects)
		require.NotNil(t, item.PublishedAt, "successful publish should be timestamped")
		assert.Equal(t, 1, item.Attempts)
	}

	assert.Equal(t, "new A", d.FindPageByID(p1.ID).Title)
	assert.Equal(t, "new B", d.FindPageByID(p2.ID).Title)
	assert.Equal(t, "new C", d.FindPageByID(p3.ID).Title)
}

func TestManagerSucceedsWhenTitleEmptyOrUnchanged(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()
	d := app.Dao()

	site := "PF NoChange Site"
	pEmpty := createTestPage(t, d, "https://example.com/empty", "keep me", site)
	pSame := createTestPage(t, d, "https://example.com/same", "same title", site)

	f := newScriptFetcher()
	f.scriptPage(pEmpty.ID, func(int) (dao.PageTitleFetchResult, error) {
		return dao.PageTitleFetchResult{Title: "", FinalURL: "https://example.com/empty"}, nil
	})
	f.scriptPage(pSame.ID, func(int) (dao.PageTitleFetchResult, error) {
		return dao.PageTitleFetchResult{Title: "same title", FinalURL: "https://example.com/same"}, nil
	})

	m := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f.Fetch))
	defer m.Close()
	job, err := m.Start(site)
	require.NoError(t, err)

	waitUntil(t, 5*time.Second, func() bool {
		j, _, _ := m.Get(job.ID, false)
		return j.Status == entity.PageFetchJobCompleted
	})

	counts := m.Counts(job.ID)
	assert.Equal(t, 2, counts.Succeeded)
	assert.Zero(t, counts.Failed)
	assert.Zero(t, counts.Skipped)

	assert.Equal(t, "keep me", d.FindPageByID(pEmpty.ID).Title)
	assert.Equal(t, "same title", d.FindPageByID(pSame.ID).Title)
}

func TestManagerRetriesOnlyFailedItems(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()
	d := app.Dao()

	site := "PF Retry Site"
	p1 := createTestPage(t, d, "https://example.com/r1", "old 1", site)
	p2 := createTestPage(t, d, "https://example.com/r2", "old 2", site)
	p3 := createTestPage(t, d, "https://example.com/r3", "old 3", site)

	f := newScriptFetcher()
	f.scriptPage(p2.ID, func(attempt int) (dao.PageTitleFetchResult, error) {
		if attempt == 1 {
			return dao.PageTitleFetchResult{}, context.DeadlineExceeded
		}
		return dao.PageTitleFetchResult{Title: "new 2", FinalURL: "https://example.com/r2"}, nil
	})

	m := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f.Fetch))
	defer m.Close()
	job, err := m.Start(site)
	require.NoError(t, err)

	waitUntil(t, 5*time.Second, func() bool {
		j, _, _ := m.Get(job.ID, false)
		return j.Status == entity.PageFetchJobCompleted && m.Counts(job.ID).Failed == 1
	})

	_, items, _ := m.Get(job.ID, true)
	var failedItem entity.PageFetchJobItem
	for _, item := range items {
		if item.PageID == p2.ID {
			failedItem = item
			assert.Equal(t, entity.PageFetchItemFailed, item.Status)
			assert.Contains(t, item.Message, "deadline exceeded")
		}
	}
	require.NotZero(t, failedItem.ID)
	assert.Equal(t, "old 2", d.FindPageByID(p2.ID).Title)

	// Retry failed only.
	reopened, err := m.RetryFailed(job.ID)
	require.NoError(t, err)
	assert.Equal(t, entity.PageFetchJobRunning, reopened.Status)

	waitUntil(t, 5*time.Second, func() bool {
		j, _, _ := m.Get(job.ID, false)
		return j.Status == entity.PageFetchJobCompleted
	})

	counts := m.Counts(job.ID)
	assert.Equal(t, 3, counts.Succeeded)
	assert.Zero(t, counts.Failed)
	assert.Zero(t, counts.Skipped)

	// Succeeded pages were fetched once, the failed one twice.
	assert.Equal(t, 1, f.callCount(p1.ID))
	assert.Equal(t, 2, f.callCount(p2.ID))
	assert.Equal(t, 1, f.callCount(p3.ID))
	assert.Equal(t, "new 2", d.FindPageByID(p2.ID).Title)

	// Nothing left to retry.
	_, err = m.RetryFailed(job.ID)
	assert.ErrorIs(t, err, pagefetch.ErrNoFailedItems)
}

func TestManagerSkipsLateResultsAfterAdminChanges(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(t *testing.T, d *dao.Dao, page entity.Page)
		wantMsg string
	}{
		{
			name: "manual title edit",
			mutate: func(t *testing.T, d *dao.Dao, page entity.Page) {
				page.Title = "manual title"
				require.NoError(t, d.UpdatePage(&page))
			},
			wantMsg: "title was modified manually",
		},
		{
			name: "page renamed",
			mutate: func(t *testing.T, d *dao.Dao, page entity.Page) {
				page.Key = "https://example.com/renamed-" + string(rune('0'+page.ID%10))
				require.NoError(t, d.UpdatePage(&page))
			},
			wantMsg: "renamed",
		},
		{
			name: "page moved to another site",
			mutate: func(t *testing.T, d *dao.Dao, page entity.Page) {
				page.SiteName = "Site B"
				require.NoError(t, d.UpdatePage(&page))
			},
			wantMsg: "moved to another site",
		},
		{
			name: "page deleted",
			mutate: func(t *testing.T, d *dao.Dao, page entity.Page) {
				require.NoError(t, d.DelPage(&page))
			},
			wantMsg: "deleted",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app, err := test.NewTestApp()
			require.NoError(t, err)
			defer app.Cleanup()
			d := app.Dao()

			site := "PF Skip Site " + tc.name
			page := createTestPage(t, d, "https://example.com/skip", "snapshot title", site)

			f := newScriptFetcher()
			reached, release := f.gatePage(page.ID)
			f.scriptPage(page.ID, func(int) (dao.PageTitleFetchResult, error) {
				return dao.PageTitleFetchResult{Title: "late fetched title", FinalURL: page.Key}, nil
			})

			m := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f.Fetch))
			defer m.Close()
			job, err := m.Start(site)
			require.NoError(t, err)

			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("fetcher was never reached")
			}

			// Admin changes the page while the fetch is in flight.
			tc.mutate(t, d, page)
			close(release)

			waitUntil(t, 5*time.Second, func() bool {
				j, _, _ := m.Get(job.ID, false)
				return j.Status == entity.PageFetchJobCompleted
			})

			counts := m.Counts(job.ID)
			assert.Zero(t, counts.Succeeded, "late result must not be published")
			assert.Equal(t, 1, counts.Skipped)
			assert.Zero(t, counts.Failed)

			_, items, err := m.Get(job.ID, true)
			require.NoError(t, err)
			require.Len(t, items, 1)
			assert.Equal(t, entity.PageFetchItemSkipped, items[0].Status)
			assert.Contains(t, items[0].Message, tc.wantMsg)
			assert.Nil(t, items[0].PublishedAt)

			switch tc.name {
			case "manual title edit":
				assert.Equal(t, "manual title", d.FindPageByID(page.ID).Title, "manual title must survive")
			case "page renamed":
				assert.Equal(t, "snapshot title", d.FindPageByID(page.ID).Title, "title must be untouched")
			case "page deleted":
				assert.True(t, d.FindPageByID(page.ID).IsEmpty(), "page must not be recreated")
			}
		})
	}
}

func TestManagerConcurrentStartSameScopeReturnsRunningJob(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()
	d := app.Dao()

	siteA := "PF Concurrent A"
	pagesA := make([]entity.Page, 0, 3)
	for _, k := range []string{"a1", "a2", "a3"} {
		pagesA = append(pagesA, createTestPage(t, d, "https://a.example/"+k, "old "+k, siteA))
	}
	siteB := "PF Concurrent B"
	pagesB := make([]entity.Page, 0, 2)
	for _, k := range []string{"b1", "b2"} {
		pagesB = append(pagesB, createTestPage(t, d, "https://b.example/"+k, "old "+k, siteB))
	}

	f := newScriptFetcher()
	// Block the first page of each site so both jobs stay in progress.
	reachedA, releaseA := f.gatePage(pagesA[0].ID)
	reachedB, releaseB := f.gatePage(pagesB[0].ID)

	m := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f.Fetch))
	defer m.Close()

	jobA, err := m.Start(siteA)
	require.NoError(t, err)
	select {
	case <-reachedA:
	case <-time.After(5 * time.Second):
		t.Fatal("site A worker never started")
	}

	// Same scope while running: the same job is returned, not a new one.
	jobADup, err := m.Start(siteA)
	require.NoError(t, err)
	assert.Equal(t, jobA.ID, jobADup.ID)

	// Different site runs independently and is not blocked.
	jobB, err := m.Start(siteB)
	require.NoError(t, err)
	assert.NotEqual(t, jobA.ID, jobB.ID)
	select {
	case <-reachedB:
	case <-time.After(5 * time.Second):
		t.Fatal("site B worker was blocked by site A job")
	}

	close(releaseA)
	close(releaseB)

	waitUntil(t, 5*time.Second, func() bool {
		jA, _, _ := m.Get(jobA.ID, false)
		jB, _, _ := m.Get(jobB.ID, false)
		return jA.Status == entity.PageFetchJobCompleted && jB.Status == entity.PageFetchJobCompleted
	})

	assert.Equal(t, 3, m.Counts(jobA.ID).Succeeded)
	assert.Equal(t, 2, m.Counts(jobB.ID).Succeeded)
}

func TestManagerResumesAfterRestartWithoutRefetchingDonePages(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()
	d := app.Dao()

	site := "PF Resume Site"
	p1 := createTestPage(t, d, "https://example.com/s1", "old 1", site)
	p2 := createTestPage(t, d, "https://example.com/s2", "old 2", site)
	p3 := createTestPage(t, d, "https://example.com/s3", "old 3", site)

	f1 := newScriptFetcher()
	_, release1 := f1.gatePage(p1.ID)
	reached2, release2 := f1.gatePage(p2.ID)
	pages := []entity.Page{p1, p2, p3}
	titles := map[uint]string{p1.ID: "new fetched-1", p2.ID: "new fetched-2", p3.ID: "new fetched-3"}
	for _, p := range pages {
		p := p
		f1.scriptPage(p.ID, func(int) (dao.PageTitleFetchResult, error) {
			return dao.PageTitleFetchResult{Title: titles[p.ID], FinalURL: p.Key}, nil
		})
	}
	m1 := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f1.Fetch))

	job, err := m1.Start(site)
	require.NoError(t, err)

	// Let page 1 finish, then crash while page 2 is in flight.
	close(release1)
	waitUntil(t, 5*time.Second, func() bool {
		_, items, _ := m1.Get(job.ID, true)
		for _, item := range items {
			if item.PageID == p1.ID && item.Status == entity.PageFetchItemSucceeded {
				return true
			}
		}
		return false
	})
	select {
	case <-reached2:
	case <-time.After(5 *time.Second):
		t.Fatal("page 2 never started")
	}
	m1.Close() // simulates process shutdown mid-request

	var staleItem entity.PageFetchJobItem
	require.NoError(t, d.DB().Where("job_id = ? AND page_id = ?", job.ID, p2.ID).First(&staleItem).Error)
	assert.Equal(t, entity.PageFetchItemFetching, staleItem.Status, "in-flight item stays unconfirmed")

	// New process: a fresh manager resumes the original job.
	f2 := newScriptFetcher()
	m2 := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f2.Fetch))
	defer m2.Close()
	m2.Resume()
	close(release2) // release the old (cancelled) fetch so it does not linger

	waitUntil(t, 5*time.Second, func() bool {
		j, _, _ := m2.Get(job.ID, false)
		return j.Status == entity.PageFetchJobCompleted
	})

	counts := m2.Counts(job.ID)
	assert.Equal(t, 3, counts.Total)
	assert.Equal(t, 3, counts.Succeeded)
	assert.Zero(t, counts.Failed)
	assert.Zero(t, counts.Skipped)
	assert.Zero(t, counts.Remaining())

	// The already completed page is not fetched again.
	assert.Zero(t, f2.callCount(p1.ID))
	assert.Equal(t, 1, f2.callCount(p2.ID))
	assert.Equal(t, 1, f2.callCount(p3.ID))

	assert.Equal(t, "new fetched-1", d.FindPageByID(p1.ID).Title)
}

func TestManagerEmptyScopeCompletesImmediately(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()

	m := pagefetch.NewManager(app.Dao())
	defer m.Close()
	job, err := m.Start("PF Site Without Pages")
	require.NoError(t, err)
	assert.Equal(t, entity.PageFetchJobCompleted, job.Status)

	counts := m.Counts(job.ID)
	assert.Zero(t, counts.Total)

	jobs, err := m.List("", 10)
	require.NoError(t, err)
	assert.NotEmpty(t, jobs)
}

func TestManagerInvalidURLFailsItemWithoutRequest(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()
	d := app.Dao()

	site := "PF Broken URL Site"
	d.NewSite(site, "")
	bad := createTestPage(t, d, "not-a-valid-url", "bad title", site)
	empty := createTestPage(t, d, "", "empty title", site)

	// Production fetcher: invalid URLs fail before any HTTP request is made.
	m := pagefetch.NewManager(d)
	defer m.Close()
	job, err := m.Start(site)
	require.NoError(t, err)

	waitUntil(t, 5*time.Second, func() bool {
		j, _, _ := m.Get(job.ID, false)
		return j.Status == entity.PageFetchJobCompleted
	})

	counts := m.Counts(job.ID)
	assert.Equal(t, 2, counts.Failed)
	assert.Zero(t, counts.Succeeded)

	_, items, err := m.Get(job.ID, true)
	require.NoError(t, err)
	msgs := map[uint]string{}
	for _, item := range items {
		assert.Equal(t, entity.PageFetchItemFailed, item.Status)
		msgs[item.PageID] = item.Message
	}
	assert.Contains(t, msgs[bad.ID], "invalid URL")
	assert.Contains(t, msgs[empty.ID], "URL cannot be null")
}

func TestManagerLatestPrefersRunningJob(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()
	d := app.Dao()

	site := "PF Latest Site"
	page := createTestPage(t, d, "https://example.org/latest", "old", site)

	f := newScriptFetcher()
	reached, release := f.gatePage(page.ID)
	m := pagefetch.NewManager(d, pagefetch.WithTitleFetcher(f.Fetch))
	defer m.Close()

	job, err := m.Start(site)
	require.NoError(t, err)
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never started")
	}

	running, err := m.Latest(site)
	require.NoError(t, err)
	require.NotNil(t, running)
	assert.Equal(t, job.ID, running.ID)
	assert.Equal(t, entity.PageFetchJobRunning, running.Status)

	close(release)
	waitUntil(t, 5*time.Second, func() bool {
		j, _, _ := m.Get(job.ID, false)
		return j.Status == entity.PageFetchJobCompleted
	})

	latest, err := m.Latest(site)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, job.ID, latest.ID)
	assert.Equal(t, entity.PageFetchJobCompleted, latest.Status)

	missing, err := m.Latest("PF Scope Without Any Job")
	require.NoError(t, err)
	assert.Nil(t, missing)
}
