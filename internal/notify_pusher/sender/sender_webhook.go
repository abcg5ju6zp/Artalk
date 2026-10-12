package sender

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/artalkjs/artalk/v2/internal/log"
)

type NotifyWebHookReqBody struct {
	NotifySubject string      `json:"notify_subject"`
	NotifyBody    string      `json:"notify_body"`
	Comment       interface{} `json:"comment"`
	ParentComment interface{} `json:"parent_comment"`
}

// SendWebHook pushes a notification.
// A nil error is returned only when the endpoint responds with 2xx;
// a timeout error means the delivery result is unconfirmed.
func SendWebHook(rawURL string, reqData *NotifyWebHookReqBody) error {
	jsonByte, _ := json.Marshal(reqData)
	statusCode, body, err := doRequest(http.MethodPost, rawURL, bytes.NewReader(jsonByte))
	if err != nil {
		log.Error("[WebHook Push] ", "Failed to send msg:", err)
		return err
	}

	if statusCode != http.StatusOK {
		err := fmt.Errorf("webhook respond status %d: %s", statusCode, body)
		log.Error("[WebHook Push] Failed to send msg:", err)
		return err
	}

	return nil
}
