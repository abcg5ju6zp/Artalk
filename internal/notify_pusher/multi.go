package notify_pusher

import (
	"html"
	"time"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/i18n"
	"github.com/artalkjs/artalk/v2/internal/log"
	"github.com/artalkjs/artalk/v2/internal/notify_pusher/sender"
	"github.com/artalkjs/artalk/v2/internal/template"
)

// multiPushEnabled reports whether any multi-push provider is turned on.
func (pusher *NotifyPusher) multiPushEnabled() bool {
	return pusher.helperServiceCount > 0 ||
		pusher.conf.Lark.Enabled ||
		pusher.conf.Bark.Enabled
}

// pushMultiPush sends the notification via Telegram / DingTalk / Slack / LINE,
// Lark and Bark, and returns the aggregated observable delivery status.
//
// A failure of the helper services is always recorded as unknown: individual
// receivers may still have gotten the message, so the channel must be retried
// rather than assumed failed.
func (pusher *NotifyPusher) pushMultiPush(comment *entity.Comment, pComment *entity.Comment, subject, body string) (string, string) {
	log.Debug(time.Now(), " 多元推送")

	statuses := []string{}
	errMsg := ""
	collect := func(status string, err error) {
		if err != nil {
			log.Error("[Notify]", err)
			if errMsg == "" {
				errMsg = err.Error()
			}
		}
		statuses = append(statuses, status)
	}

	// Telegram / DingTalk / Slack / LINE
	if pusher.helperServiceCount > 0 {
		if err := pusher.helper.Send(pusher.ctx, subject, html.EscapeString(body)); err != nil {
			collect(entity.DeliveryUnknown, err)
		} else {
			collect(entity.DeliverySent, nil)
		}
	}

	// 飞书
	if pusher.conf.Lark.Enabled {
		if err := sender.SendLark(pusher.conf.Lark.WebhookURL, subject, body, pusher.conf.Lark.MsgType == "card"); err != nil {
			collect(classifyTimeoutErr(err), err)
		} else {
			collect(entity.DeliverySent, nil)
		}
	}

	// Bark
	if pusher.conf.Bark.Enabled {
		if err := sender.SendBark(pusher.conf.Bark.Server, subject, body); err != nil {
			collect(classifyTimeoutErr(err), err)
		} else {
			collect(entity.DeliverySent, nil)
		}
	}

	return mergeDeliveryStatus(statuses...), errMsg
}

// pushWebhook sends the standalone WebHook notification.
func (pusher *NotifyPusher) pushWebhook(subject string, body string, comment *entity.Comment, pComment *entity.Comment) (string, string) {
	var pCommentCooked entity.CookedComment
	if pComment != nil && !pComment.IsEmpty() {
		pCommentCooked = pusher.dao.CookComment(pComment)
	}

	err := sender.SendWebHook(pusher.conf.WebHook.URL, &sender.NotifyWebHookReqBody{
		NotifySubject: subject,
		NotifyBody:    body,
		Comment:       pusher.dao.CookComment(comment),
		ParentComment: pCommentCooked,
	})
	if err != nil {
		return classifyTimeoutErr(err), err.Error()
	}
	return entity.DeliverySent, ""
}

func classifyTimeoutErr(err error) string {
	if sender.IsTimeoutErr(err) {
		return entity.DeliveryUnknown
	}
	return entity.DeliveryFailed
}

func (pusher *NotifyPusher) getAdminNotifySubjectBody(comment *entity.Comment, toUserID uint) (string, string) {
	// 评论内容文字截断
	// coContent := lib.TruncateString(comment.Content, 280)
	// if len([]rune(coContent)) > 280 {
	// 	coContent = coContent + "..."
	// }

	render := template.NewRenderer(pusher.dao, template.TYPE_NOTIFY, template.NewFileLoader(pusher.conf.NotifyTpl))
	notify := pusher.dao.FindCreateNotify(toUserID, comment.ID)

	var subject string
	if pusher.conf.NotifySubject != "" {
		subject = render.Render(&notify, pusher.conf.NotifySubject)
	}

	body := render.Render(&notify)
	if comment.IsPending {
		body = "[" + i18n.T("Pending") + "]\n\n" + body
	}

	return subject, body
}
