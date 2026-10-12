package entity

import (
	"time"

	"gorm.io/gorm"
)

// Post-processing stage states of a created comment
const (
	PostStagePending uint8 = 0 // not finished yet, can be (re)run
	PostStageDone    uint8 = 1 // finished
	PostStageFailed  uint8 = 2 // gave up after retries, does not block other stages
)

// Notification channels, each channel keeps its own observable delivery result
const (
	PostChannelEmail     = "email"     // reply emails and admin emails
	PostChannelWebhook   = "webhook"   // WebHook push
	PostChannelMultiPush = "multipush" // Telegram / DingTalk / Slack / LINE / Lark / Bark
)

// Delivery results of a single notification channel
const (
	DeliveryPending = "pending" // dispatch started, result is not confirmed yet, can be retried
	DeliverySent    = "sent"    // delivery confirmed, must never be sent again
	DeliveryFailed  = "failed"  // definite failure, becomes terminal after the retry cap
	DeliveryUnknown = "unknown" // timeout or ambiguous response, can be retried
	DeliverySkipped = "skipped" // rules decided it does not apply, terminal
)

// CommentPostTask is the recoverable processing identity kept for every created comment.
//
// It binds page completion, anti-spam verdict and notifications to the same comment,
// so an interrupted process can resume the unfinished stages without replaying
// the confirmed ones.
type CommentPostTask struct {
	gorm.Model

	CommentID uint `gorm:"uniqueIndex"`

	// Request identity captured at comment creation
	IP      string
	UA      string
	Referer string
	IsAdmin bool

	// Stage states
	PageState     uint8 `gorm:"default:0"`
	PageAttempts  int
	AntiSpamState uint8 `gorm:"default:0"`
	NotifyState   uint8 `gorm:"default:0"`

	LastError  string `gorm:"size:500"`
	FinishedAt *time.Time
}

// CommentPostDelivery stores the observable result of one notification channel
// bound to one comment. The unique (comment_id, channel) key is the idempotency
// guard: only channels without a confirmed result are processed on resume.
type CommentPostDelivery struct {
	gorm.Model

	CommentID uint   `gorm:"index;uniqueIndex:idx_comment_post_delivery"`
	Channel   string `gorm:"size:32;uniqueIndex:idx_comment_post_delivery"`

	Status    string `gorm:"size:16;index"`
	Attempts  int
	LastError string `gorm:"size:500"`
	SentAt    *time.Time
}
