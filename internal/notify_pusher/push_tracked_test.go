package notify_pusher_test

import (
	"sync/atomic"
	"testing"

	"github.com/artalkjs/artalk/v2/internal/config"
	"github.com/artalkjs/artalk/v2/internal/dao"
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/notify_pusher"
	"github.com/libtnb/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type daoRecorder struct {
	d         *dao.Dao
	commentID uint
}

func (r *daoRecorder) IsDone(channel string) bool {
	return r.d.IsPostDeliveryTerminal(r.commentID, channel)
}

func (r *daoRecorder) Record(channel, status, errMsg string) {
	_ = r.d.UpsertPostDelivery(r.commentID, channel, status, errMsg)
}

func newPusherTestDao(t *testing.T) *dao.Dao {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	return dao.NewDao(db)
}

// A confirmed email must never be sent again on resume, while an unconfirmed
// email is retried once.
func TestPushTrackedEmailIdempotency(t *testing.T) {
	d := newPusherTestDao(t)

	parent := entity.User{Name: "parent", Email: "parent@example.org", ReceiveEmail: true}
	author := entity.User{Name: "author", Email: "author@example.org"}
	require.NoError(t, d.CreateUser(&parent))
	require.NoError(t, d.CreateUser(&author))

	page := entity.Page{Key: "/p.html", SiteName: "site", Title: "t"}
	require.NoError(t, d.CreatePage(&page))

	parentComment := entity.Comment{Content: "parent", PageKey: page.Key, SiteName: "site", UserID: parent.ID}
	require.NoError(t, d.CreateComment(&parentComment))
	reply := entity.Comment{
		Content: "reply", PageKey: page.Key, SiteName: "site", UserID: author.ID,
		Rid: parentComment.ID,
	}
	require.NoError(t, d.CreateComment(&reply))

	var pushed int32
	pusher := notify_pusher.NewNotifyPusher(&notify_pusher.NotifyPusherConf{
		AdminNotifyConf: config.AdminNotifyConf{
			Email: &config.AdminEmailConf{Enabled: true},
		},
		Dao: d,
		EmailPush: func(notify *entity.Notify) error {
			atomic.AddInt32(&pushed, 1)
			return nil // accepted by the email queue, not confirmed yet
		},
	})
	rec := &daoRecorder{d: d, commentID: reply.ID}

	// first run: email is enqueued but unconfirmed
	pusher.PushTracked(&reply, &parentComment, rec)
	assert.Equal(t, int32(1), atomic.LoadInt32(&pushed))
	email := d.FindPostDelivery(reply.ID, entity.PostChannelEmail)
	assert.Equal(t, entity.DeliveryPending, email.Status)

	// resume before confirmation: the unconfirmed channel is retried
	pusher.PushTracked(&reply, &parentComment, rec)
	assert.Equal(t, int32(2), atomic.LoadInt32(&pushed))

	// SMTP confirms delivery
	for _, n := range d.FindNotifiesByComment(reply.ID) {
		require.NoError(t, d.NotifySetEmailed(&n))
	}
	require.NoError(t, d.UpsertPostDelivery(reply.ID, entity.PostChannelEmail, entity.DeliverySent, ""))

	// later resumes never send the confirmed email again
	pusher.PushTracked(&reply, &parentComment, rec)
	pusher.PushTracked(&reply, &parentComment, rec)
	assert.Equal(t, int32(2), atomic.LoadInt32(&pushed), "confirmed email must not be duplicated")

	email = d.FindPostDelivery(reply.ID, entity.PostChannelEmail)
	assert.Equal(t, entity.DeliverySent, email.Status)
}
