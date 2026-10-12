package dao

import (
	"errors"
	"time"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"gorm.io/gorm"
)

// MaxPostDeliveryAttempts is the retry cap after which an uncertain channel
// result becomes a terminal failure so post-processing tasks can converge.
const MaxPostDeliveryAttempts = 3

// ===============
//  CommentPostTask
// ===============

func (dao *Dao) CreatePostTask(task *entity.CommentPostTask) error {
	return dao.DB().Create(task).Error
}

func (dao *Dao) FindPostTask(commentID uint) entity.CommentPostTask {
	var task entity.CommentPostTask
	dao.DB().Where("comment_id = ?", commentID).First(&task)
	return task
}

// FindPendingPostTasks returns tasks that have not reached a terminal state.
func (dao *Dao) FindPendingPostTasks(limit int) []entity.CommentPostTask {
	var tasks []entity.CommentPostTask
	q := dao.DB().Where("finished_at IS NULL").Order("id ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	q.Find(&tasks)
	return tasks
}

func (dao *Dao) SetPostTaskPageResult(id uint, state uint8, attempts int, errMsg string) error {
	updates := map[string]any{
		"page_state":    state,
		"page_attempts": attempts,
	}
	if errMsg != "" {
		updates["last_error"] = truncatePostError(errMsg)
	}
	return dao.DB().Model(&entity.CommentPostTask{}).Where("id = ?", id).Updates(updates).Error
}

func (dao *Dao) SetPostTaskAntiSpamState(id uint, state uint8) error {
	return dao.DB().Model(&entity.CommentPostTask{}).Where("id = ?", id).
		Update("anti_spam_state", state).Error
}

func (dao *Dao) SetPostTaskNotifyState(id uint, state uint8) error {
	return dao.DB().Model(&entity.CommentPostTask{}).Where("id = ?", id).
		Update("notify_state", state).Error
}

func (dao *Dao) FinishPostTask(id uint, note string) error {
	now := time.Now()
	updates := map[string]any{"finished_at": now}
	if note != "" {
		updates["last_error"] = truncatePostError(note)
	}
	return dao.DB().Model(&entity.CommentPostTask{}).Where("id = ?", id).Updates(updates).Error
}

// ===============
//  CommentPostDelivery
// ===============

func (dao *Dao) FindPostDelivery(commentID uint, channel string) entity.CommentPostDelivery {
	var delivery entity.CommentPostDelivery
	dao.DB().Where("comment_id = ? AND channel = ?", commentID, channel).First(&delivery)
	return delivery
}

// IsPostDeliveryTerminal reports whether the channel no longer needs processing:
// sent and skipped are definitive; failed is terminal after the retry cap.
func (dao *Dao) IsPostDeliveryTerminal(commentID uint, channel string) bool {
	delivery := dao.FindPostDelivery(commentID, channel)
	if delivery.ID == 0 {
		return false
	}
	switch delivery.Status {
	case entity.DeliverySent, entity.DeliverySkipped, entity.DeliveryFailed:
		return true
	default:
		return false
	}
}

// HasUnconfirmedPostDelivery reports whether any channel is still waiting for a
// confirmed result (email queue draining, timeout, ambiguous response, etc.).
func (dao *Dao) HasUnconfirmedPostDelivery(commentID uint) bool {
	var count int64
	dao.DB().Model(&entity.CommentPostDelivery{}).
		Where("comment_id = ? AND status IN ?", commentID,
			[]string{entity.DeliveryPending, entity.DeliveryUnknown}).
		Count(&count)
	return count > 0
}

// UpsertPostDelivery records the latest observable result of one channel.
//
// A confirmed "sent" result is never downgraded. Non-terminal results consume an
// attempt; once the retry cap is reached the result turns into a terminal failure.
func (dao *Dao) UpsertPostDelivery(commentID uint, channel, status, errMsg string) error {
	var delivery entity.CommentPostDelivery
	err := dao.DB().Where("comment_id = ? AND channel = ?", commentID, channel).First(&delivery).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		delivery = entity.CommentPostDelivery{
			CommentID: commentID,
			Channel:   channel,
			Status:    status,
			LastError: truncatePostError(errMsg),
		}
		if status != entity.DeliverySent && status != entity.DeliverySkipped {
			delivery.Attempts = 1
			if delivery.Attempts >= MaxPostDeliveryAttempts {
				delivery.Status = entity.DeliveryFailed
			}
		}
		if status == entity.DeliverySent {
			now := time.Now()
			delivery.SentAt = &now
		}
		return dao.DB().Create(&delivery).Error
	}
	if err != nil {
		return err
	}

	// confirmed deliveries must never be sent twice
	if delivery.Status == entity.DeliverySent {
		return nil
	}

	if status != entity.DeliverySent && status != entity.DeliverySkipped {
		delivery.Attempts++
		if delivery.Attempts >= MaxPostDeliveryAttempts {
			status = entity.DeliveryFailed
			if errMsg == "" {
				errMsg = "max delivery attempts reached"
			}
		}
	}
	delivery.Status = status
	delivery.LastError = truncatePostError(errMsg)
	if status == entity.DeliverySent {
		now := time.Now()
		delivery.SentAt = &now
	}
	return dao.DB().Save(&delivery).Error
}

// SkipRemainingPostDeliveries marks non-confirmed channels as skipped.
//
// Called when late processing reads the latest comment state and finds the
// comment was collapsed or deleted: those sends are no longer applicable.
func (dao *Dao) SkipRemainingPostDeliveries(commentID uint, reason string) {
	for _, channel := range []string{entity.PostChannelEmail, entity.PostChannelWebhook, entity.PostChannelMultiPush} {
		delivery := dao.FindPostDelivery(commentID, channel)
		if delivery.ID == 0 {
			_ = dao.UpsertPostDelivery(commentID, channel, entity.DeliverySkipped, reason)
			continue
		}
		if delivery.Status == entity.DeliverySent || delivery.Status == entity.DeliverySkipped {
			continue // never revoke a confirmed send
		}
		delivery.Status = entity.DeliverySkipped
		delivery.LastError = truncatePostError(reason)
		_ = dao.DB().Save(&delivery).Error
	}
}

// RefreshPostEmailDelivery confirms the email channel once every Notify record
// bound to the comment has been emailed. Un-emailed records keep the channel
// unconfirmed so a restart only re-sends what was not confirmed.
func (dao *Dao) RefreshPostEmailDelivery(commentID uint) {
	notifies := dao.FindNotifiesByComment(commentID)
	if len(notifies) == 0 {
		return
	}
	for _, n := range notifies {
		if !n.IsEmailed {
			return
		}
	}
	_ = dao.UpsertPostDelivery(commentID, entity.PostChannelEmail, entity.DeliverySent, "")
}

func (dao *Dao) FindNotifiesByComment(commentID uint) []entity.Notify {
	var notifies []entity.Notify
	dao.DB().Where("comment_id = ?", commentID).Find(&notifies)
	return notifies
}

func truncatePostError(msg string) string {
	const maxLen = 500
	if len(msg) > maxLen {
		return msg[:maxLen]
	}
	return msg
}
