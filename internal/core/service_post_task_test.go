package core_test

import (
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/artalkjs/artalk/v2/internal/config"
	"github.com/artalkjs/artalk/v2/internal/core"
	"github.com/artalkjs/artalk/v2/internal/dao"
	dbLogger "github.com/artalkjs/artalk/v2/internal/db/logger"
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/pkged"
	"github.com/libtnb/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newPostTaskTestApp builds a self-contained app on an in-memory database.
// It avoids the shared fixtures app whose Asia/Shanghai config conflicts with
// the process-wide timezone initializer once other core tests ran first.
func newPostTaskTestApp(t *testing.T) *core.App {
	t.Helper()

	// run with the project root as workdir, like the shared test app
	if _, filename, _, ok := runtime.Caller(0); ok {
		rootDir := path.Join(path.Dir(filename), "..", "..")
		require.NoError(t, os.Chdir(rootDir))
	}
	pkged.SetFS(os.DirFS("./"))

	dbInstance, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: dbLogger.New(),
	})
	require.NoError(t, err)

	conf := &config.Config{
		TimeZone: "Local",
		DB:       config.DBConf{Type: config.TypeSQLite, Dsn: "file::memory:?cache=shared"},
		Cache:    config.CacheConf{Enabled: false, Type: config.CacheTypeBuiltin},
		SiteDefault: "Default Site",
		AdminNotify: config.AdminNotifyConf{
			Email: &config.AdminEmailConf{Enabled: true},
		},
	}

	app := core.NewApp(conf)
	app.SetDao(dao.NewDao(dbInstance))
	require.NoError(t, app.Bootstrap())

	seedPostTaskData(t, app)

	t.Cleanup(func() { _ = app.ResetBootstrapState() })
	return app
}

func seedPostTaskData(t *testing.T, app *core.App) uint {
	d := app.Dao()

	site := entity.Site{Name: "Site A", Urls: ""}
	require.NoError(t, d.CreateSite(&site))
	require.NoError(t, d.CreatePage(&entity.Page{Key: "/test/1000.html", Title: "titled page", SiteName: site.Name}))

	admin := entity.User{Name: "admin", Email: "admin@example.org", IsAdmin: true, ReceiveEmail: true}
	require.NoError(t, d.CreateUser(&admin))
	user := entity.User{Name: "userA", Email: "user_a@example.org", ReceiveEmail: true}
	require.NoError(t, d.CreateUser(&user))
	return user.ID
}

func postTaskService(t *testing.T, app *core.App) *core.PostTaskService {
	svc, err := core.AppService[*core.PostTaskService](app)
	require.NoError(t, err)
	return svc
}

func newPostTestComment(t *testing.T, app *core.App, rid uint) entity.Comment {
	user := app.Dao().FindUser("userA", "user_a@example.org")
	require.False(t, user.IsEmpty())
	comment := entity.Comment{
		Content:  "hello post task",
		PageKey:  "/test/1000.html", // titled page, so no network fetch is attempted
		SiteName: "Site A",
		UserID:   user.ID, // non-admin seeded user
		Rid:      rid,
	}
	require.NoError(t, app.Dao().CreateComment(&comment))
	return comment
}

func waitTaskFinished(t *testing.T, app *core.App, commentID uint) entity.CommentPostTask {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task := app.Dao().FindPostTask(commentID)
		if task.ID != 0 && task.FinishedAt != nil {
			return task
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("post task for comment %d did not finish", commentID)
	return entity.CommentPostTask{}
}

// The normal one-shot path converges without external services configured:
// every stage settles and every channel records an observable result.
func TestPostTaskNormalConvergence(t *testing.T) {
	app := newPostTaskTestApp(t)

	comment := newPostTestComment(t, app, 0)
	postTaskService(t, app).Dispatch(comment, core.PostTaskParams{
		IP: "127.0.0.1", UA: "ua", Referer: "https://example.org",
	})

	task := waitTaskFinished(t, app, comment.ID)
	assert.Equal(t, entity.PostStageDone, task.PageState, "page completion must not block other stages")
	assert.Equal(t, entity.PostStageDone, task.AntiSpamState)
	assert.Equal(t, entity.PostStageDone, task.NotifyState)

	// each channel keeps its own observable result
	for _, channel := range []string{
		entity.PostChannelEmail, entity.PostChannelWebhook, entity.PostChannelMultiPush,
	} {
		delivery := app.Dao().FindPostDelivery(comment.ID, channel)
		require.NotZero(t, delivery.ID, "missing delivery record for %s", channel)
		assert.True(t,
			delivery.Status == entity.DeliverySent ||
				delivery.Status == entity.DeliverySkipped ||
				delivery.Status == entity.DeliveryFailed,
			"channel %s should be terminal, got %s", channel, delivery.Status)
	}
	assert.False(t, app.Dao().HasUnconfirmedPostDelivery(comment.ID))
}

// A late task must read the latest comment state: an admin collapse stops
// every send that has not been confirmed yet.
func TestPostTaskLateCollapseStopsSends(t *testing.T) {
	app := newPostTaskTestApp(t)

	comment := newPostTestComment(t, app, 0)
	require.NoError(t, app.Dao().CreatePostTask(&entity.CommentPostTask{CommentID: comment.ID}))

	comment.IsCollapsed = true
	require.NoError(t, app.Dao().UpdateComment(&comment))

	postTaskService(t, app).Recover()
	waitTaskFinished(t, app, comment.ID)

	for _, channel := range []string{
		entity.PostChannelEmail, entity.PostChannelWebhook, entity.PostChannelMultiPush,
	} {
		delivery := app.Dao().FindPostDelivery(comment.ID, channel)
		require.NotZero(t, delivery.ID)
		assert.Equal(t, entity.DeliverySkipped, delivery.Status,
			"channel %s must be skipped for a collapsed comment", channel)
		assert.Contains(t, delivery.LastError, "collapsed")
	}
}

// A late task targeting a deleted comment terminates without sending.
func TestPostTaskDeletedCommentTerminates(t *testing.T) {
	app := newPostTaskTestApp(t)

	comment := newPostTestComment(t, app, 0)
	require.NoError(t, app.Dao().CreatePostTask(&entity.CommentPostTask{CommentID: comment.ID}))
	require.NoError(t, app.Dao().DelComment(&comment))

	postTaskService(t, app).Recover()
	task := waitTaskFinished(t, app, comment.ID)
	assert.Contains(t, task.LastError, "deleted")
}

// On resume, confirmed channels are left untouched while unconfirmed ones are
// processed, with no duplicate sends for confirmed channels.
func TestPostTaskResumeOnlyUnconfirmedChannels(t *testing.T) {
	app := newPostTaskTestApp(t)

	comment := newPostTestComment(t, app, 0)
	require.NoError(t, app.Dao().CreatePostTask(&entity.CommentPostTask{CommentID: comment.ID}))

	// webhook confirmed before the process exited; multipush result unknown
	require.NoError(t, app.Dao().UpsertPostDelivery(comment.ID, entity.PostChannelWebhook, entity.DeliverySent, ""))
	require.NoError(t, app.Dao().UpsertPostDelivery(comment.ID, entity.PostChannelMultiPush, entity.DeliveryUnknown, "timeout"))

	postTaskService(t, app).Recover()
	waitTaskFinished(t, app, comment.ID)

	webhook := app.Dao().FindPostDelivery(comment.ID, entity.PostChannelWebhook)
	assert.Equal(t, entity.DeliverySent, webhook.Status)
	assert.Zero(t, webhook.Attempts, "confirmed channel must not be retried")

	multi := app.Dao().FindPostDelivery(comment.ID, entity.PostChannelMultiPush)
	assert.NotEqual(t, entity.DeliveryUnknown, multi.Status, "uncertain channel must be re-processed")
}

// Admin comments skip the anti-spam stage while still completing normally.
func TestPostTaskAdminCommentSkipsAntiSpam(t *testing.T) {
	app := newPostTaskTestApp(t)

	comment := newPostTestComment(t, app, 0)
	postTaskService(t, app).Dispatch(comment, core.PostTaskParams{IsAdmin: true})

	task := waitTaskFinished(t, app, comment.ID)
	assert.Equal(t, entity.PostStageDone, task.AntiSpamState)
	assert.NotNil(t, task.FinishedAt)
}
