package sender

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

// httpClient bounds push requests so a hanging endpoint cannot pin a
// post-processing task forever.
var httpClient = &http.Client{Timeout: 10 * time.Second}

func doRequest(method, rawURL string, body io.Reader) (int, string, error) {
	req, err := http.NewRequest(method, rawURL, body)
	if err != nil {
		return 0, "", err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respBody), nil
}

// IsTimeoutErr reports whether the failure is a timeout, in which case the
// remote side may still have received the message and the result is unknown.
func IsTimeoutErr(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}
