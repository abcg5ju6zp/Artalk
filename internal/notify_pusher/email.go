package notify_pusher

import (
	"errors"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/log"
	"golang.org/x/exp/slices"
)

func (pusher *NotifyPusher) sendEmail(notify *entity.Notify) error {
	if pusher.conf.EmailPush == nil {
		return ErrEmailPushUnavailable
	}
	return pusher.conf.EmailPush(notify)
}

// pushEmailsToUser sends the reply notification to the parent comment author.
// Returns the delivery status of the email channel.
func (pusher *NotifyPusher) pushEmailsToUser(comment *entity.Comment, pComment *entity.Comment) string {
	if !pusher.checkNeedSendEmailToUser(comment, pComment) {
		log.Debug("ignore email notify by pusher.checkNeedSendEmailToUser")
		return entity.DeliverySkipped
	}

	notify := pusher.dao.FindCreateNotify(pComment.UserID, comment.ID)
	if notify.IsEmailed {
		// already confirmed on a previous run, never send it twice
		return entity.DeliverySent
	}
	pusher.dao.NotifySetInitial(&notify)

	if err := pusher.sendEmail(&notify); err != nil {
		log.Error("[Notify] email to user enqueue err: ", err)
		return pusher.classifyEmailErr(err)
	}
	return entity.DeliveryPending
}

// pushEmailsToAdmins sends notifications to every applicable admin.
func (pusher *NotifyPusher) pushEmailsToAdmins(comment *entity.Comment, pComment *entity.Comment) string {
	result := entity.DeliverySkipped

	toAddrSent := []string{} // 记录已发送的收件人地址（避免重复发送）
	for _, admin := range pusher.dao.GetAllAdmins() {
		// 该管理员地址已曾发送，避免重复发送
		if slices.Contains(toAddrSent, admin.Email) {
			continue
		}
		toAddrSent = append(toAddrSent, admin.Email)

		if !pusher.checkNeedSendEmailToAdmin(comment, pComment, &admin) {
			log.Debug("ignore email notify by pusher.checkNeedSendEmailToAdmin")
			continue
		}

		notify := pusher.dao.FindCreateNotify(admin.ID, comment.ID)
		if notify.IsEmailed {
			result = entity.DeliverySent
			continue // 已确认送达的管理员不重复发送
		}
		pusher.dao.NotifySetInitial(&notify)

		if err := pusher.sendEmail(&notify); err != nil {
			log.Error("[Notify] email to admin enqueue err: ", err)
			if result != entity.DeliveryPending {
				result = pusher.classifyEmailErr(err)
			}
			continue
		}
		result = entity.DeliveryPending
	}

	return result
}

// pushEmails aggregates the reply-email and admin-email results.
func (pusher *NotifyPusher) pushEmails(comment *entity.Comment, pComment *entity.Comment, isRoot bool) string {
	results := []string{}
	if !isRoot {
		results = append(results, pusher.pushEmailsToUser(comment, pComment))
	}
	if isRoot || pusher.conf.NoiseMode {
		results = append(results, pusher.pushEmailsToAdmins(comment, pComment))
	}
	return mergeDeliveryStatus(results...)
}

// classifyEmailErr maps the email bridge error onto a delivery status.
// A disabled or unconfigured service is "not applicable" rather than a failure.
func (pusher *NotifyPusher) classifyEmailErr(err error) string {
	if errors.Is(err, ErrEmailPushUnavailable) || errors.Is(err, ErrEmailDisabled) {
		return entity.DeliverySkipped
	}
	return entity.DeliveryFailed
}
