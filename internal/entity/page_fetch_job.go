package entity

import (
	"time"

	"gorm.io/gorm"
)

const (
	PageFetchJobStatusRunning   = "running"
	PageFetchJobStatusCompleted = "completed"
)

const (
	PageFetchItemStatusPending    = "pending"
	PageFetchItemStatusProcessing = "processing"
	PageFetchItemStatusSuccess    = "success"
	PageFetchItemStatusFailed     = "failed"
	PageFetchItemStatusSkipped    = "skipped"
)

// Reasons why a fetch result is not allowed to publish
const (
	PageFetchSkipReasonDeleted      = "page_deleted"
	PageFetchSkipReasonSiteChanged  = "site_changed"
	PageFetchSkipReasonKeyChanged   = "key_changed"
	PageFetchSkipReasonTitleChanged = "title_changed"
)

// PageFetchJob is a persisted batch "fetch all pages" job.
//
// The scope (SiteName, empty means all sites) and the page/title versions are
// fixed when the job is created, so the job can be resumed after a restart and
// late fetch results cannot overwrite admin changes.
type PageFetchJob struct {
	gorm.Model

	SiteName string `gorm:"index;size:255"` // Job scope, empty means all sites
	Status   string `gorm:"index;size:32"`

	Total   int // Number of pages snapshotted when the job started
	Success int
	Failed  int
	Skipped int
}

// PageFetchJobItem records the per-page fetch and publish result of a job.
type PageFetchJobItem struct {
	gorm.Model

	JobID  uint `gorm:"index"`
	PageID uint `gorm:"index"`

	// Snapshot of the page identity and title version at job start
	PageKey      string `gorm:"size:255"`
	SiteName     string `gorm:"size:255"`
	TitleVersion int

	Status   string `gorm:"index;size:32"`
	Attempts int

	// Fetch result
	Title       string     // Title extracted from the remote page
	Redirected  bool       // Whether an HTTP or meta-refresh redirect was followed
	FinalURL    string     `gorm:"size:1024"` // Final URL after redirects
	Error       string     `gorm:"size:1024"` // Failure reason (status == failed)
	SkipReason  string     `gorm:"size:32"`   // Why the result was skipped (status == skipped)
	PublishedAt *time.Time // When the fetched title was published to the DB and cache
}
