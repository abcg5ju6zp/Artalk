package notify_pusher

import (
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/log"
)

// 通知发送 (from comment to parentComment)
func (pusher *NotifyPusher) Push(comment *entity.Comment, pComment *entity.Comment) {
	pusher.PushTracked(comment, pComment, noopRecorder{})
}

// PushTracked runs every notification channel and records its observable result
// via the recorder, so that on resume only channels without a terminal result
// are processed and confirmed deliveries are never sent twice.
func (pusher *NotifyPusher) PushTracked(comment *entity.Comment, pComment *entity.Comment, rec DeliveryRecorder) {
	isRootComment := comment.Rid == 0

	// ==============
	//  邮件 (回复对方 + 通知管理员)
	// ==============
	if !rec.IsDone(entity.PostChannelEmail) {
		rec.Record(entity.PostChannelEmail, pusher.pushEmails(comment, pComment, isRootComment), "")
	}

	// 多元推送与 WebHook 共用同一组审核/适用规则
	if !pusher.checkNeedMultiPush(comment, pComment) {
		pusher.recordSkipIfNotDone(rec, entity.PostChannelMultiPush)
		pusher.recordSkipIfNotDone(rec, entity.PostChannelWebhook)
		return
	}

	// 通知消息
	allAdmins := pusher.dao.GetAllAdminIDs()
	if len(allAdmins) == 0 {
		log.Warn("Multi push disabled: no admin user found")
		pusher.recordSkipIfNotDone(rec, entity.PostChannelMultiPush)
		pusher.recordSkipIfNotDone(rec, entity.PostChannelWebhook)
		return
	}

	firstAdminUser := allAdmins[0]
	subject, body := pusher.getAdminNotifySubjectBody(comment, firstAdminUser)

	// ==============
	//  管理员多元推送
	// ==============
	if !rec.IsDone(entity.PostChannelMultiPush) {
		if !pusher.multiPushEnabled() {
			rec.Record(entity.PostChannelMultiPush, entity.DeliverySkipped, "")
		} else {
			status, errMsg := pusher.pushMultiPush(comment, pComment, subject, body)
			rec.Record(entity.PostChannelMultiPush, status, errMsg)
		}
	}

	// ==============
	//  WebHook
	// ==============
	if !rec.IsDone(entity.PostChannelWebhook) {
		if !pusher.conf.WebHook.Enabled {
			rec.Record(entity.PostChannelWebhook, entity.DeliverySkipped, "")
		} else {
			status, errMsg := pusher.pushWebhook(subject, body, comment, pComment)
			rec.Record(entity.PostChannelWebhook, status, errMsg)
		}
	}
}

func (pusher *NotifyPusher) recordSkipIfNotDone(rec DeliveryRecorder, channel string) {
	if !rec.IsDone(channel) {
		rec.Record(channel, entity.DeliverySkipped, "")
	}
}
