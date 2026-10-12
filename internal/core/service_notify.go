package core

import (
	"errors"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/notify_pusher"
)

// errNotifyUnavailable is returned before the pusher is initialized.
var errNotifyUnavailable = errors.New("notify pusher is not initialized")

var _ Service = (*NotifyService)(nil)

type NotifyService struct {
	app    *App
	pusher *notify_pusher.NotifyPusher
}

func NewNotifyService(app *App) *NotifyService {
	return &NotifyService{app: app}
}

func (s *NotifyService) Init() error {
	s.pusher = notify_pusher.NewNotifyPusher(&notify_pusher.NotifyPusherConf{
		AdminNotifyConf: s.app.Conf().AdminNotify,
		Dao:             s.app.Dao(),
		EmailPush: func(notify *entity.Notify) error {
			emailService, err := AppService[*EmailService](s.app)
			if err != nil {
				return err
			}
			if err := emailService.AsyncSend(notify); err != nil {
				if errors.Is(err, ErrEmailDisabled) {
					return notify_pusher.ErrEmailDisabled
				}
				return err
			}
			return nil
		},
	})

	return nil
}

func (s *NotifyService) Dispose() error {
	s.pusher = nil

	return nil
}

func (s *NotifyService) Push(comment *entity.Comment, pComment *entity.Comment) error {
	if s.pusher == nil {
		return errNotifyUnavailable
	}
	s.pusher.Push(comment, pComment)
	return nil
}

// PushTracked runs the notification channels while recording per-channel
// observable delivery results.
func (s *NotifyService) PushTracked(comment *entity.Comment, pComment *entity.Comment, recorder notify_pusher.DeliveryRecorder) error {
	if s.pusher == nil {
		return errNotifyUnavailable
	}
	s.pusher.PushTracked(comment, pComment, recorder)
	return nil
}
