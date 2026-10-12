package sender

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/artalkjs/artalk/v2/internal/log"
)

// SendBark pushes a Bark notification.
// A nil error is returned only when the endpoint responds with 2xx;
// a timeout error means the delivery result is unconfirmed.
func SendBark(serverURL string, title string, msg string) error {
	if title == "" {
		title = "Artalk"
	}

	rawURL := fmt.Sprintf("%s/%s/%s", strings.TrimSuffix(serverURL, "/"), url.QueryEscape(title), url.QueryEscape(msg))
	statusCode, body, err := doRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		log.Error("[Bark] ", "Failed to send msg:", err)
		return err
	}

	if statusCode != http.StatusOK {
		err := fmt.Errorf("bark respond status %d: %s", statusCode, body)
		log.Error("[Bark] Failed to send msg:", err)
		return err
	}

	return nil
}
