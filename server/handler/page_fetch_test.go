package handler_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/artalkjs/artalk/v2/server/common"
	"github.com/artalkjs/artalk/v2/server/handler"
)

func TestPageFetchJobAPI(t *testing.T) {
	app, fiber := NewApiTestApp()
	defer app.Cleanup()

	handler.PageFetchAll(app.App, fiber)
	handler.PageFetchStatus(app.App, fiber)
	handler.PageFetchJobList(app.App, fiber)
	handler.PageFetchJobGet(app.App, fiber)
	handler.PageFetchJobRetry(app.App, fiber)

	admin := app.Dao().FindUserByID(1000)
	require.False(t, admin.IsEmpty())
	token, err := common.LoginGetUserToken(admin, app.Conf().AppKey, 3600)
	require.NoError(t, err)

	doReq := func(method, url, body string) *http.Response {
		var reader io.Reader
		if body != "" {
			reader = bytes.NewReader([]byte(body))
		}
		req := httptest.NewRequest(method, url, reader)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := fiber.Test(req)
		require.NoError(t, err)
		return resp
	}

	decode := func(resp *http.Response) map[string]interface{} {
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var data map[string]interface{}
		require.NoError(t, json.Unmarshal(raw, &data), string(raw))
		return data
	}

	// Start a job for an empty scope.
	resp := doReq(http.MethodPost, "/pages/fetch", `{"site_name":"Empty Fetch Site"}`)
	assert.Equal(t, 200, resp.StatusCode)
	job := decode(resp)
	assert.Equal(t, "completed", job["status"])
	assert.EqualValues(t, 0, job["total"])
	jobID, ok := job["id"].(float64)
	require.True(t, ok)

	// Legacy status polling keeps working and exposes the new counters.
	resp = doReq(http.MethodGet, "/pages/fetch/status?site_name=Empty+Fetch+Site", "")
	assert.Equal(t, 200, resp.StatusCode)
	status := decode(resp)
	assert.Equal(t, false, status["is_progress"])
	assert.EqualValues(t, jobID, status["job_id"])
	assert.EqualValues(t, 0, status["done"])
	assert.EqualValues(t, 0, status["total"])
	assert.EqualValues(t, 0, status["success"])
	assert.EqualValues(t, 0, status["failed"])
	assert.EqualValues(t, 0, status["skipped"])
	assert.EqualValues(t, 0, status["remaining"])
	assert.NotEmpty(t, status["msg"])

	// Jobs are queryable.
	resp = doReq(http.MethodGet, "/pages/fetch/jobs?site_name=Empty+Fetch+Site", "")
	assert.Equal(t, 200, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var list []map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &list))
	require.Len(t, list, 1)
	assert.EqualValues(t, jobID, list[0]["id"])

	resp = doReq(http.MethodGet, "/pages/fetch/jobs/"+strconv.Itoa(int(jobID)), "")
	assert.Equal(t, 200, resp.StatusCode)
	detail := decode(resp)
	assert.EqualValues(t, jobID, detail["id"])
	assert.Equal(t, "Empty Fetch Site", detail["site_name"])

	// Retry with no failed items is rejected.
	resp = doReq(http.MethodPost, "/pages/fetch/jobs/"+strconv.Itoa(int(jobID))+"/retry", "{}")
	assert.Equal(t, 400, resp.StatusCode)

	// Unknown job.
	resp = doReq(http.MethodGet, "/pages/fetch/jobs/999999", "")
	assert.Equal(t, 404, resp.StatusCode)
}

func TestPageFetchJobAPIRequiresAdmin(t *testing.T) {
	app, fiber := NewApiTestApp()
	defer app.Cleanup()

	handler.PageFetchAll(app.App, fiber)
	handler.PageFetchJobList(app.App, fiber)

	req := httptest.NewRequest(http.MethodPost, "/pages/fetch", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := fiber.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 403, resp.StatusCode)

	req = httptest.NewRequest(http.MethodGet, "/pages/fetch/jobs", nil)
	resp, err = fiber.Test(req)
	require.NoError(t, err)
	assert.Equal(t, 403, resp.StatusCode)
}
