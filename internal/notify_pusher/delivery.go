package notify_pusher

import (
	"errors"

	"github.com/artalkjs/artalk/v2/internal/entity"
)

// Sentinel errors for the email push bridge.
var (
	// ErrEmailPushUnavailable is returned when no email bridge is configured.
	ErrEmailPushUnavailable = errors.New("email push bridge is not available")

	// ErrEmailDisabled is returned by the bridge when the email service is turned off.
	// Such a result is "not applicable" rather than a retryable failure.
	ErrEmailDisabled = errors.New("email service is disabled")
)

// DeliveryRecorder persists the observable result of every notification channel
// bound to one comment, so that on resume only unconfirmed channels are processed.
type DeliveryRecorder interface {
	// IsDone reports whether the channel already reached a terminal state
	// (sent / skipped / failed) and must not be processed again.
	IsDone(channel string) bool
	// Record persists the latest delivery result of the channel.
	Record(channel string, status string, errMsg string)
}

type noopRecorder struct{}

func (noopRecorder) IsDone(string) bool                    { return false }
func (noopRecorder) Record(string, string, string) {}

// mergeDeliveryStatus picks the most severe status:
// unknown > failed > pending > sent > skipped.
func mergeDeliveryStatus(statuses ...string) string {
	weight := map[string]int{
		entity.DeliverySkipped: 0,
		entity.DeliverySent:    1,
		entity.DeliveryPending: 2,
		entity.DeliveryFailed:  3,
		entity.DeliveryUnknown: 4,
	}
	result := entity.DeliverySkipped
	for _, s := range statuses {
		if weight[s] > weight[result] {
			result = s
		}
	}
	return result
}
