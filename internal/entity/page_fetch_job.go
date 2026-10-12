package entity

import (
	"time"

	"gorm.io/gorm"
)

const (
	PageFetchJobRunning   = "running"
	PageFetchJobCompleted = "completed"
)

const (
	PageFetchItemPending   = "pending"
	PageFetchItemFetching  = "fetching"
	PageFetchItemSucceeded = "succeeded"
	PageFetchItemFailed    = "failed"
	PageFetchItemSkipped   = "skipped"
)

// PageFetchJob persists a single "fetch all pages" batch so that its scope,
// per-page results and progress survive process restarts.
type PageFetchJob struct {
	gorm.Model

	SiteName string `gorm:"index;size:255"` // Snapshot of the scope, empty string means all sites
	Status   string `gorm:"index;size:32"`  // PageFetchJobRunning | PageFetchJobCompleted
}

// PageFetchJobItem records the fetch and publish result of one page inside a job.
type PageFetchJobItem struct {
	gorm.Model

	JobID  uint `gorm:"index;uniqueIndex:idx_page_fetch_job_page"`
	PageID uint `gorm:"uniqueIndex:idx_page_fetch_job_page"`

	PageKey       string `gorm:"size:255"`  // Snapshot of the page key when the job started
	SiteName      string `gorm:"size:255"`  // Snapshot of the site the page belonged to when the job started
	TitleSnapshot string `gorm:"type:text"` // Snapshot of the title when the job started (title version)

	Status   string `gorm:"index;size:32"` // PageFetchItemPending | ... | PageFetchItemSkipped
	Attempts int

	Redirects    int    // Number of html redirects followed while fetching
	FinalURL     string `gorm:"size:1024"` // Final URL after redirects
	FetchedTitle string `gorm:"type:text"` // Title fetched from the URL
	Message      string `gorm:"type:text"` // Failure or skip reason

	PublishedAt *time.Time // Time when the fetched title was published to the db and cache
}
