package dao_test

import (
	"testing"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostTaskLifecycle(t *testing.T) {
	app, _ := test.NewTestApp()
	defer app.Cleanup()
	d := app.Dao()

	task := entity.CommentPostTask{CommentID: 900001, IP: "127.0.0.1", UA: "ua", IsAdmin: false}
	require.NoError(t, d.CreatePostTask(&task))
	require.NotZero(t, task.ID)

	// it can be found by comment id
	found := d.FindPostTask(900001)
	assert.Equal(t, task.ID, found.ID)
	assert.Equal(t, entity.PostStagePending, found.PageState)

	// unfinished tasks are recoverable
	pending := d.FindPendingPostTasks(0)
	var ids []uint
	for _, p := range pending {
		ids = append(ids, p.CommentID)
	}
	assert.Contains(t, ids, uint(900001))

	// stage results persist independently
	require.NoError(t, d.SetPostTaskPageResult(task.ID, entity.PostStageDone, 1, ""))
	require.NoError(t, d.SetPostTaskAntiSpamState(task.ID, entity.PostStageDone))
	require.NoError(t, d.SetPostTaskNotifyState(task.ID, entity.PostStageDone))

	stored := d.FindPostTask(900001)
	assert.Equal(t, entity.PostStageDone, stored.PageState)
	assert.Equal(t, entity.PostStageDone, stored.AntiSpamState)
	assert.Equal(t, entity.PostStageDone, stored.NotifyState)

	// finishing removes it from the recovery set
	require.NoError(t, d.FinishPostTask(task.ID, ""))
	stored = d.FindPostTask(900001)
	require.NotNil(t, stored.FinishedAt)
	assert.NotContains(t, commentIDs(d.FindPendingPostTasks(0)), uint(900001))
}

func TestPostDeliveryUpsertSemantics(t *testing.T) {
	app, _ := test.NewTestApp()
	defer app.Cleanup()
	d := app.Dao()

	const commentID = uint(900002)

	// first uncertain result is recorded with one attempt
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelWebhook, entity.DeliveryUnknown, "timeout"))
	delivery := d.FindPostDelivery(commentID, entity.PostChannelWebhook)
	assert.Equal(t, entity.DeliveryUnknown, delivery.Status)
	assert.Equal(t, 1, delivery.Attempts)
	assert.False(t, d.IsPostDeliveryTerminal(commentID, entity.PostChannelWebhook))
	assert.True(t, d.HasUnconfirmedPostDelivery(commentID))

	// another uncertain result consumes an attempt
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelWebhook, entity.DeliveryUnknown, "timeout"))
	delivery = d.FindPostDelivery(commentID, entity.PostChannelWebhook)
	assert.Equal(t, 2, delivery.Attempts)

	// reaching the retry cap turns failure terminal so the task can converge
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelWebhook, entity.DeliveryUnknown, "timeout"))
	delivery = d.FindPostDelivery(commentID, entity.PostChannelWebhook)
	assert.Equal(t, entity.DeliveryFailed, delivery.Status)
	assert.True(t, d.IsPostDeliveryTerminal(commentID, entity.PostChannelWebhook))
	assert.False(t, d.HasUnconfirmedPostDelivery(commentID))

	// a confirmed channel is never downgraded or re-counted
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelEmail, entity.DeliverySent, ""))
	email := d.FindPostDelivery(commentID, entity.PostChannelEmail)
	assert.Equal(t, entity.DeliverySent, email.Status)
	require.NotNil(t, email.SentAt)
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelEmail, entity.DeliveryPending, ""))
	email = d.FindPostDelivery(commentID, entity.PostChannelEmail)
	assert.Equal(t, entity.DeliverySent, email.Status, "sent must never be downgraded")
	assert.Zero(t, email.Attempts)

	// skipped is terminal
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelMultiPush, entity.DeliverySkipped, ""))
	assert.True(t, d.IsPostDeliveryTerminal(commentID, entity.PostChannelMultiPush))
}

func TestSkipRemainingPostDeliveries(t *testing.T) {
	app, _ := test.NewTestApp()
	defer app.Cleanup()
	d := app.Dao()

	const commentID = uint(900003)
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelEmail, entity.DeliveryPending, ""))
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelWebhook, entity.DeliverySent, ""))

	d.SkipRemainingPostDeliveries(commentID, "comment is collapsed")

	email := d.FindPostDelivery(commentID, entity.PostChannelEmail)
	assert.Equal(t, entity.DeliverySkipped, email.Status)
	webhook := d.FindPostDelivery(commentID, entity.PostChannelWebhook)
	assert.Equal(t, entity.DeliverySent, webhook.Status, "confirmed sends are preserved")

	// channels without a row are recorded as skipped too
	multi := d.FindPostDelivery(commentID, entity.PostChannelMultiPush)
	assert.Equal(t, entity.DeliverySkipped, multi.Status)
}

func TestRefreshPostEmailDelivery(t *testing.T) {
	app, _ := test.NewTestApp()
	defer app.Cleanup()
	d := app.Dao()

	const commentID = uint(900004)

	// two recipients, one still un-emailed: channel stays unconfirmed
	n1 := d.FindCreateNotify(1000, commentID)
	n2 := d.FindCreateNotify(1001, commentID)
	require.NoError(t, d.NotifySetInitial(&n1))
	require.NoError(t, d.NotifySetInitial(&n2))
	require.NoError(t, d.NotifySetEmailed(&n1))
	require.NoError(t, d.UpsertPostDelivery(commentID, entity.PostChannelEmail, entity.DeliveryPending, ""))

	d.RefreshPostEmailDelivery(commentID)
	assert.False(t, d.IsPostDeliveryTerminal(commentID, entity.PostChannelEmail))
	assert.True(t, d.HasUnconfirmedPostDelivery(commentID))

	// all recipients confirmed: channel becomes sent and the task may finish
	require.NoError(t, d.NotifySetEmailed(&n2))
	d.RefreshPostEmailDelivery(commentID)
	assert.True(t, d.IsPostDeliveryTerminal(commentID, entity.PostChannelEmail))
	email := d.FindPostDelivery(commentID, entity.PostChannelEmail)
	assert.Equal(t, entity.DeliverySent, email.Status)
	assert.False(t, d.HasUnconfirmedPostDelivery(commentID))
}

func TestSetPostTaskPageFailureKeepsOtherStagesIndependent(t *testing.T) {
	app, _ := test.NewTestApp()
	defer app.Cleanup()
	d := app.Dao()

	task := entity.CommentPostTask{CommentID: 900005}
	require.NoError(t, d.CreatePostTask(&task))

	require.NoError(t, d.SetPostTaskPageResult(task.ID, entity.PostStagePending, 1, "fetch failed"))
	stored := d.FindPostTask(900005)
	assert.Equal(t, entity.PostStagePending, stored.PageState)
	assert.Contains(t, stored.LastError, "fetch failed")

	// other stages are still pending and runnable
	assert.Equal(t, entity.PostStagePending, stored.AntiSpamState)
	assert.Equal(t, entity.PostStagePending, stored.NotifyState)
}

func commentIDs(tasks []entity.CommentPostTask) []uint {
	ids := make([]uint, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.CommentID)
	}
	return ids
}
