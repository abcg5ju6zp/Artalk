// Package pagefetch persists a "refresh all pages" batch as a resumable job.
//
// A job fixes the site scope and a per-page snapshot (key, site and title
// version) at start time. Each page is fetched and published independently;
// a late fetch result is only published when the page still belongs to its
// original site and neither its key nor its title version has changed.
package pagefetch

import (
	"context"
	"errors"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/artalkjs/artalk/v2/internal/dao"
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/log"
)

var (
	// ErrJobNotFound is returned when no job matches the given id.
	ErrJobNotFound = errors.New("page fetch job not found")
	// ErrNoFailedItems is returned when a retry is requested but nothing failed.
	ErrNoFailedItems = errors.New("no failed page to retry")
)

// TitleFetcher resolves a page title for the current state of a page.
// It is a seam so that the worker does not depend on real HTTP in tests.
type TitleFetcher func(ctx context.Context, page *entity.Page) (dao.PageTitleFetchResult, error)

// NewDefaultTitleFetcher builds the production fetcher: resolve the accessible
// URL from the current page/site state, then perform the HTTP title request.
func NewDefaultTitleFetcher(d *dao.Dao) TitleFetcher {
	return func(ctx context.Context, page *entity.Page) (dao.PageTitleFetchResult, error) {
		rawURL := d.CookPage(page).URL
		if rawURL == "" {
			return dao.PageTitleFetchResult{}, dao.ErrPageFetchURLEmpty
		}
		return dao.FetchPageTitleDetail(ctx, rawURL)
	}
}

// Option customizes a Manager.
type Option func(*Manager)

// WithTitleFetcher overrides the HTTP title fetcher (used in tests).
func WithTitleFetcher(f TitleFetcher) Option {
	return func(m *Manager) { m.fetcher = f }
}

// Manager owns the in-process workers and coordinates them with the persisted
// job state in the database.
type Manager struct {
	dao     *dao.Dao
	fetcher TitleFetcher

	mu      sync.Mutex
	runners map[uint]bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewManager creates a manager around the given dao.
func NewManager(d *dao.Dao, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		dao:     d,
		fetcher: NewDefaultTitleFetcher(d),
		runners: map[uint]bool{},
		ctx:     ctx,
		cancel:  cancel,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Start snapshots the scope and begins a new job, or returns the running job of
// the same scope when a concurrent start requests it. An empty siteName fixes
// the scope to all sites.
func (m *Manager) Start(siteName string) (*entity.PageFetchJob, error) {
	m.mu.Lock()

	// Concurrent start of the same scope returns the executing job.
	var existing entity.PageFetchJob
	err := m.dao.DB().
		Where("site_name = ? AND status = ?", siteName, entity.PageFetchJobRunning).
		Order("id DESC").First(&existing).Error
	if err == nil {
		m.mu.Unlock()
		m.ensureRunner(existing.ID)
		return &existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		m.mu.Unlock()
		return nil, err
	}

	// Snapshot the pending pages of the scope at job start.
	var pages []entity.Page
	query := m.dao.DB().Model(&entity.Page{}).Order("id ASC")
	if siteName != "" {
		query = query.Where(&entity.Page{SiteName: siteName})
	}
	if err := query.Find(&pages).Error; err != nil {
		m.mu.Unlock()
		return nil, err
	}

	job := &entity.PageFetchJob{SiteName: siteName, Status: entity.PageFetchJobRunning}
	items := make([]entity.PageFetchJobItem, 0, len(pages))
	for _, p := range pages {
		items = append(items, entity.PageFetchJobItem{
			PageID:        p.ID,
			PageKey:       p.Key,
			SiteName:      p.SiteName,
			TitleSnapshot: p.Title,
			Status:        entity.PageFetchItemPending,
		})
	}

	err = m.dao.DB().Transaction(func(tx *gorm.DB) error {
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
		} else {
			// An empty scope finishes immediately without spawning a worker.
			job.Status = entity.PageFetchJobCompleted
			return tx.Model(job).Update("status", entity.PageFetchJobCompleted).Error
		}
		return nil
	})
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}

	m.ensureRunner(job.ID)
	return job, nil
}

// ensureRunner launches a single worker for the job unless one is already active.
func (m *Manager) ensureRunner(jobID uint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runners[jobID] || m.ctx.Err() != nil {
		return
	}
	m.runners[jobID] = true
	m.wg.Add(1)
	go m.run(jobID)
}

// run drains the unconfirmed items of a job; completed items are never refetched.
func (m *Manager) run(jobID uint) {
	defer func() {
		m.mu.Lock()
		delete(m.runners, jobID)
		m.mu.Unlock()
		m.wg.Done()
	}()

	for {
		if m.ctx.Err() != nil {
			return
		}

		var item entity.PageFetchJobItem
		err := m.dao.DB().
			Where("job_id = ? AND status IN ?", jobID,
				[]string{entity.PageFetchItemPending, entity.PageFetchItemFetching}).
			Order("id ASC").First(&item).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			m.completeJob(jobID)
			return
		}
		if err != nil {
			log.Error("[PageFetchJob] claim item error: ", err)
			time.Sleep(time.Second)
			continue
		}

		// Mark as fetching and count this attempt. A "fetching" item claimed here
		// belongs to a previous process that died mid-request and is retried once.
		if err := m.dao.DB().Model(&item).Updates(map[string]interface{}{
			"status":   entity.PageFetchItemFetching,
			"attempts": gorm.Expr("attempts + ?", 1),
		}).Error; err != nil {
			log.Error("[PageFetchJob] mark item fetching error: ", err)
		}

		m.processItem(&item)
	}
}

// completeJob finishes a job only when every item has reached a terminal state.
func (m *Manager) completeJob(jobID uint) {
	var remaining int64
	if err := m.dao.DB().Model(&entity.PageFetchJobItem{}).
		Where("job_id = ? AND status IN ?", jobID,
			[]string{entity.PageFetchItemPending, entity.PageFetchItemFetching}).
		Count(&remaining).Error; err != nil {
		log.Error("[PageFetchJob] count remaining items error: ", err)
		return
	}
	if remaining > 0 {
		return
	}
	if err := m.dao.DB().Model(&entity.PageFetchJob{}).
		Where("id = ? AND status = ?", jobID, entity.PageFetchJobRunning).
		Update("status", entity.PageFetchJobCompleted).Error; err != nil {
		log.Error("[PageFetchJob] complete job error: ", err)
	}
}

// processItem fetches one page and conditionally publishes its title.
func (m *Manager) processItem(item *entity.PageFetchJobItem) {
	var page entity.Page
	err := m.dao.DB().First(&page, item.PageID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		m.saveItem(item.ID, entity.PageFetchItemSkipped, "page was deleted", nil)
		return
	} else if err != nil {
		m.saveItem(item.ID, entity.PageFetchItemFailed, err.Error(), nil)
		return
	}

	// Identity and title-version pre-check: do not touch the network for results
	// that are already known to be late.
	if reason := staleReason(item, &page); reason != "" {
		m.saveItem(item.ID, entity.PageFetchItemSkipped, reason, nil)
		return
	}

	result, err := m.fetcher(m.ctx, &page)
	if err != nil {
		if m.ctx.Err() != nil {
			// The process is shutting down. Leave the item unconfirmed ("fetching")
			// so it is retried after restart instead of being marked failed.
			return
		}
		m.saveItem(item.ID, entity.PageFetchItemFailed, err.Error(), map[string]interface{}{
			"redirects": result.Redirects,
			"final_url": result.FinalURL,
		})
		return
	}

	fields := map[string]interface{}{
		"redirects":     result.Redirects,
		"final_url":     result.FinalURL,
		"fetched_title": result.Title,
	}

	if result.Title == "" {
		// Keep the previous title, same as the historical single-fetch behavior.
		m.saveItem(item.ID, entity.PageFetchItemSucceeded, "", fields)
		return
	}

	outcome, err := m.dao.PublishFetchedPageTitle(
		item.PageID, item.PageKey, item.SiteName, item.TitleSnapshot, result.Title)
	if err != nil {
		m.saveItem(item.ID, entity.PageFetchItemFailed, err.Error(), fields)
		return
	}

	switch outcome {
	case dao.PageTitlePublished:
		now := time.Now()
		fields["published_at"] = &now
		m.saveItem(item.ID, entity.PageFetchItemSucceeded, "", fields)
	case dao.PageTitleUnchanged:
		// No title update needed; count it as a successful fetch.
		m.saveItem(item.ID, entity.PageFetchItemSucceeded, "", fields)
	default:
		m.saveItem(item.ID, entity.PageFetchItemSkipped, m.staleReason(item), fields)
	}
}

// staleReason explains why an item's snapshot no longer matches the page.
func staleReason(item *entity.PageFetchJobItem, page *entity.Page) string {
	switch {
	case page.Key != item.PageKey:
		return "page was renamed"
	case page.SiteName != item.SiteName:
		return "page was moved to another site"
	case page.Title != item.TitleSnapshot:
		return "page title was modified manually"
	default:
		return ""
	}
}

// staleReason reloads the page to explain a publish-time skip.
func (m *Manager) staleReason(item *entity.PageFetchJobItem) string {
	var page entity.Page
	err := m.dao.DB().First(&page, item.PageID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "page was deleted before publishing"
	}
	if err != nil {
		return "stale result skipped"
	}
	if reason := staleReason(item, &page); reason != "" {
		return reason + " before publishing"
	}
	return "stale result skipped"
}

// saveItem persists a terminal item state; progress is therefore crash-safe.
func (m *Manager) saveItem(itemID uint, status, message string, extra map[string]interface{}) {
	values := map[string]interface{}{
		"status":  status,
		"message": message,
	}
	for k, v := range extra {
		values[k] = v
	}
	if err := m.dao.DB().Model(&entity.PageFetchJobItem{}).
		Where("id = ?", itemID).Updates(values).Error; err != nil {
		log.Error("[PageFetchJob] save item error: ", err)
	}
}

// RetryFailed reopens a finished job and retries failed items only; succeeded
// and skipped items are left untouched. A running job is simply returned.
func (m *Manager) RetryFailed(jobID uint) (*entity.PageFetchJob, error) {
	var job entity.PageFetchJob
	if err := m.dao.DB().First(&job, jobID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	if job.Status == entity.PageFetchJobRunning {
		m.ensureRunner(job.ID)
		return &job, nil
	}

	m.mu.Lock()
	result := m.dao.DB().Model(&entity.PageFetchJobItem{}).
		Where("job_id = ? AND status = ?", jobID, entity.PageFetchItemFailed).
		Updates(map[string]interface{}{"status": entity.PageFetchItemPending})
	if result.Error != nil {
		m.mu.Unlock()
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		m.mu.Unlock()
		return &job, ErrNoFailedItems
	}
	if err := m.dao.DB().Model(&job).
		Update("status", entity.PageFetchJobRunning).Error; err != nil {
		m.mu.Unlock()
		return nil, err
	}
	job.Status = entity.PageFetchJobRunning
	m.mu.Unlock()

	m.ensureRunner(jobID)
	return &job, nil
}

// Resume continues all unfinished jobs after a process restart. Items that were
// being fetched when the process died are unconfirmed and reset to pending;
// succeeded, failed and skipped items keep their persisted state.
func (m *Manager) Resume() {
	if err := m.dao.DB().Model(&entity.PageFetchJobItem{}).
		Where("status = ?", entity.PageFetchItemFetching).
		Update("status", entity.PageFetchItemPending).Error; err != nil {
		log.Error("[PageFetchJob] resume reset error: ", err)
	}

	var jobs []entity.PageFetchJob
	if err := m.dao.DB().
		Where("status = ?", entity.PageFetchJobRunning).
		Find(&jobs).Error; err != nil {
		log.Error("[PageFetchJob] resume query error: ", err)
		return
	}
	for i := range jobs {
		m.ensureRunner(jobs[i].ID)
	}
}

// Close signals all workers to stop and waits for them to park state safely.
func (m *Manager) Close() {
	m.cancel()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		log.Warn("[PageFetchJob] shutdown timeout, some workers were interrupted")
	}
}

// Get loads a job and, when withItems is true, its per-page items.
func (m *Manager) Get(jobID uint, withItems bool) (*entity.PageFetchJob, []entity.PageFetchJobItem, error) {
	var job entity.PageFetchJob
	if err := m.dao.DB().First(&job, jobID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, ErrJobNotFound
		}
		return nil, nil, err
	}

	var items []entity.PageFetchJobItem
	if withItems {
		if err := m.dao.DB().
			Where("job_id = ?", jobID).Order("id ASC").Find(&items).Error; err != nil {
			return nil, nil, err
		}
	}
	return &job, items, nil
}

// List returns recent jobs, optionally restricted to a site scope.
func (m *Manager) List(siteName string, limit int) ([]entity.PageFetchJob, error) {
	if limit <= 0 {
		limit = 20
	}
	query := m.dao.DB().Model(&entity.PageFetchJob{}).Order("id DESC").Limit(limit)
	if siteName != "" {
		query = query.Where("site_name = ?", siteName)
	}
	var jobs []entity.PageFetchJob
	err := query.Find(&jobs).Error
	return jobs, err
}

// Latest returns the running job of the scope, otherwise the most recent job.
// An empty siteName considers every scope (used by the legacy status polling).
func (m *Manager) Latest(siteName string) (*entity.PageFetchJob, error) {
	scope := m.dao.DB().Model(&entity.PageFetchJob{})
	if siteName != "" {
		scope = scope.Where("site_name = ?", siteName)
	}

	var job entity.PageFetchJob
	err := scope.
		Where("status = ?", entity.PageFetchJobRunning).
		Order("id DESC").First(&job).Error
	if err == nil {
		return &job, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	err = m.dao.DB().Order("id DESC").First(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if siteName != "" && job.SiteName != siteName {
		return nil, nil
	}
	return &job, nil
}

// Counts summarizes the per-page results of a job.
type Counts struct {
	Total     int
	Pending   int
	Fetching  int
	Succeeded int
	Failed    int
	Skipped   int
}

// Remaining is the number of unconfirmed pages.
func (c Counts) Remaining() int { return c.Pending + c.Fetching }

// Counts loads the result counters of a job.
func (m *Manager) Counts(jobID uint) Counts {
	var rows []struct {
		Status string
		N      int64
	}
	m.dao.DB().Model(&entity.PageFetchJobItem{}).
		Select("status, count(*) as n").
		Where("job_id = ?", jobID).
		Group("status").
		Scan(&rows)

	var counts Counts
	for _, r := range rows {
		counts.Total += int(r.N)
		switch r.Status {
		case entity.PageFetchItemPending:
			counts.Pending += int(r.N)
		case entity.PageFetchItemFetching:
			counts.Fetching += int(r.N)
		case entity.PageFetchItemSucceeded:
			counts.Succeeded += int(r.N)
		case entity.PageFetchItemFailed:
			counts.Failed += int(r.N)
		case entity.PageFetchItemSkipped:
			counts.Skipped += int(r.N)
		}
	}
	return counts
}
