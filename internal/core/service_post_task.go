package core

import (
	"sync"
	"time"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/log"
	atkSync "github.com/artalkjs/artalk/v2/internal/sync"
)

const (
	postTaskQueueSize    = 1024
	postTaskWorkers      = 4
	postTaskShutdownWait = 5 * time.Second
	postTaskRecoverEvery = 5 * time.Minute
	postTaskPageMaxRetry = 3
)

var _ Service = (*PostTaskService)(nil)

// PostTaskParams carries the request identity captured at comment creation.
type PostTaskParams struct {
	IP      string
	UA      string
	Referer string
	IsAdmin bool
}

// PostTaskService runs the recoverable post-processing of created comments:
// page completion, anti-spam verdict and notification channels are bound to
// one persisted task per comment, so an interrupted process can resume the
// unfinished stages without replaying the confirmed channels.
type PostTaskService struct {
	app *App

	ch       chan uint
	wg       sync.WaitGroup
	keyLocks *atkSync.KeyMutex[uint]
	ticker   *time.Ticker
	stopTick chan struct{}
	closeMux sync.Mutex
	closed   bool
}

func NewPostTaskService(app *App) *PostTaskService {
	return &PostTaskService{
		app:      app,
		keyLocks: atkSync.NewKeyMutex[uint](),
	}
}

func (s *PostTaskService) Init() error {
	s.closeMux.Lock()
	s.closed = false
	s.ch = make(chan uint, postTaskQueueSize)
	s.closeMux.Unlock()

	for i := 0; i < postTaskWorkers; i++ {
		go s.worker()
	}

	// an email confirmation may finish a still-open task
	if emailService, err := AppService[*EmailService](s.app); err == nil {
		emailService.SetOnDeliveryConfirmed(func(commentID uint) { s.enqueue(commentID) })
	}

	// periodically pick up tasks that were dropped from the queue or left
	// unconfirmed; persisted state makes them safe to resume
	s.ticker = time.NewTicker(postTaskRecoverEvery)
	s.stopTick = make(chan struct{})
	go func() {
		for {
			select {
			case <-s.stopTick:
				return
			case <-s.ticker.C:
				s.Recover()
			}
		}
	}()

	return nil
}

func (s *PostTaskService) Dispose() error {
	s.closeMux.Lock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	s.closeMux.Unlock()

	if s.ticker != nil {
		s.ticker.Stop()
		close(s.stopTick)
	}

	// wait for in-flight tasks, but never block shutdown: interrupted tasks
	// stay persisted and resume on the next boot
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(postTaskShutdownWait):
		log.Warn("[PostTask] timed out waiting in-flight tasks; they will resume on next start")
	}

	return nil
}

// Dispatch persists the processing identity of a new comment and starts it.
func (s *PostTaskService) Dispatch(comment entity.Comment, params PostTaskParams) {
	task := entity.CommentPostTask{
		CommentID: comment.ID,

		IP:      params.IP,
		UA:      params.UA,
		Referer: params.Referer,
		IsAdmin: params.IsAdmin,
	}
	if err := s.app.Dao().CreatePostTask(&task); err != nil {
		log.Error("[PostTask] persist task error: ", err)
		return
	}

	s.enqueue(comment.ID)
}

// Recover enqueues every task that has not reached a terminal state.
func (s *PostTaskService) Recover() {
	dao := s.app.Dao()
	if dao == nil {
		return
	}
	tasks := dao.FindPendingPostTasks(postTaskQueueSize)
	go func() {
		for _, task := range tasks {
			s.enqueue(task.CommentID)
		}
	}()
}

func (s *PostTaskService) enqueue(commentID uint) {
	s.closeMux.Lock()
	defer s.closeMux.Unlock()

	if s.closed {
		return // stays persisted and is picked up by the next recovery
	}

	s.wg.Add(1)
	select {
	case s.ch <- commentID:
	default:
		s.wg.Done()
		log.Warn("[PostTask] queue is full, comment task deferred: ", commentID)
	}
}

func (s *PostTaskService) worker() {
	for commentID := range s.ch {
		s.runGuarded(commentID)
	}
}

func (s *PostTaskService) runGuarded(commentID uint) {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Error("[PostTask] task panic, comment=", commentID, ": ", r)
		}
	}()
	s.process(commentID)
}

func (s *PostTaskService) process(commentID uint) {
	// serialize late deliveries and recoveries targeting the same comment
	s.keyLocks.GetLock(commentID).Lock()
	defer s.keyLocks.GetLock(commentID).Unlock()

	dao := s.app.Dao()
	task := dao.FindPostTask(commentID)
	if task.ID == 0 || task.FinishedAt != nil {
		return
	}

	comment := dao.FindComment(commentID)
	if comment.IsEmpty() {
		// comment was deleted: stop every send that was not confirmed yet
		dao.SkipRemainingPostDeliveries(commentID, "comment is deleted")
		_ = dao.FinishPostTask(task.ID, "comment is deleted")
		return
	}

	// Stage 1: page completion (runs independently; its failure never blocks
	// the anti-spam or notification stages)
	var pageWg sync.WaitGroup
	if task.PageState == entity.PostStagePending {
		pageWg.Add(1)
		go func() {
			defer pageWg.Done()
			s.runPageStage(&task, &comment)
		}()
	}

	// Stage 2: anti-spam verdict; notifications are gated behind this stage
	if task.AntiSpamState == entity.PostStagePending {
		s.runAntiSpamStage(&task, &comment)
	}

	// Stage 3: notifications (only after the verdict is persisted, reading
	// the latest comment state). A collapse skips sends that no longer apply;
	// page metadata is independent and still allowed to settle.
	if task.NotifyState != entity.PostStageDone || dao.HasUnconfirmedPostDelivery(commentID) {
		s.runNotifyStage(&task, &comment)
	}

	pageWg.Wait()

	// converge only when every stage settled and every channel was confirmed;
	// otherwise the email callback or the next recovery resumes this task
	fresh := dao.FindPostTask(commentID)
	if fresh.ID == 0 {
		return
	}
	if fresh.PageState != entity.PostStagePending &&
		fresh.AntiSpamState == entity.PostStageDone &&
		fresh.NotifyState == entity.PostStageDone &&
		!dao.HasUnconfirmedPostDelivery(commentID) {
		_ = dao.FinishPostTask(fresh.ID, "")
	}
}

// runPageStage fetches the page title when needed. Failure is recorded without
// blocking the rest of the pipeline and retried on later resumes.
func (s *PostTaskService) runPageStage(task *entity.CommentPostTask, comment *entity.Comment) {
	dao := s.app.Dao()

	page := dao.FindPage(comment.PageKey, comment.SiteName)
	if dao.CookPage(&page).URL == "" || page.Title != "" {
		_ = dao.SetPostTaskPageResult(task.ID, entity.PostStageDone, task.PageAttempts, "")
		return
	}

	attempts := task.PageAttempts + 1
	if err := dao.FetchPageFromURL(&page); err != nil {
		log.Error("[PostTask] page completion failed: ", err)
		state := entity.PostStagePending
		if attempts >= postTaskPageMaxRetry {
			state = entity.PostStageFailed
		}
		_ = dao.SetPostTaskPageResult(task.ID, state, attempts, err.Error())
		return
	}

	_ = dao.SetPostTaskPageResult(task.ID, entity.PostStageDone, attempts, "")
}

// runAntiSpamStage runs the checkers with the request identity captured at
// creation; block/update callbacks persist the verdict on the comment.
func (s *PostTaskService) runAntiSpamStage(task *entity.CommentPostTask, comment *entity.Comment) {
	dao := s.app.Dao()

	if !task.IsAdmin {
		if antiSpamService, err := AppService[*AntiSpamService](s.app); err == nil {
			antiSpamService.CheckAndBlock(&AntiSpamCheckPayload{
				Comment:      comment,
				ReqReferer:   task.Referer,
				ReqIP:        task.IP,
				ReqUserAgent: task.UA,
			})
		} else {
			log.Error("[PostTask] AntiSpamService unavailable: ", err)
		}
	}

	_ = dao.SetPostTaskAntiSpamState(task.ID, entity.PostStageDone)
}

// runNotifyStage re-reads the latest comment state before sending. When the
// comment became deleted or collapsed, sends that no longer apply are stopped.
// A missing notify service leaves the stage pending for the next resume.
func (s *PostTaskService) runNotifyStage(task *entity.CommentPostTask, _ *entity.Comment) {
	dao := s.app.Dao()

	latest := dao.FindComment(task.CommentID)
	if latest.IsEmpty() {
		dao.SkipRemainingPostDeliveries(task.CommentID, "comment is deleted")
		_ = dao.SetPostTaskNotifyState(task.ID, entity.PostStageDone)
		return
	}
	if latest.IsCollapsed {
		// a late delivery must respect an admin collapse
		dao.SkipRemainingPostDeliveries(task.CommentID, "comment is collapsed")
		_ = dao.SetPostTaskNotifyState(task.ID, entity.PostStageDone)
		return
	}

	var parentComment entity.Comment
	if latest.Rid != 0 {
		parentComment = dao.FindComment(latest.Rid)
	}

	notifyService, err := AppService[*NotifyService](s.app)
	if err != nil {
		log.Error("[PostTask] NotifyService unavailable: ", err)
		return // leave the stage pending for the next resume
	}

	if err := notifyService.PushTracked(&latest, &parentComment, &postDeliveryRecorder{
		app:       s.app,
		commentID: latest.ID,
	}); err != nil {
		log.Error("[PostTask] notify push unavailable: ", err)
		return // leave the stage pending for the next resume
	}

	_ = dao.SetPostTaskNotifyState(task.ID, entity.PostStageDone)
}

// postDeliveryRecorder persists channel results through the DAO.
type postDeliveryRecorder struct {
	app       *App
	commentID uint
}

func (r *postDeliveryRecorder) IsDone(channel string) bool {
	return r.app.Dao().IsPostDeliveryTerminal(r.commentID, channel)
}

func (r *postDeliveryRecorder) Record(channel string, status string, errMsg string) {
	if err := r.app.Dao().UpsertPostDelivery(r.commentID, channel, status, errMsg); err != nil {
		log.Error("[PostTask] record delivery result error: ", err)
	}
}
