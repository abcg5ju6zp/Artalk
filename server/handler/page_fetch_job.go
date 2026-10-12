package handler

import (
	"time"

	"github.com/artalkjs/artalk/v2/internal/core"
	"github.com/artalkjs/artalk/v2/internal/dao"
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/i18n"
	"github.com/artalkjs/artalk/v2/server/common"
	"github.com/gofiber/fiber/v2"
)

type ResponsePageFetchJobItem struct {
	ID          uint       `json:"id"`
	PageID      uint       `json:"page_id"`
	PageKey     string     `json:"page_key"`
	SiteName    string     `json:"site_name"`
	Status      string     `json:"status"` // pending | processing | success | failed | skipped
	Attempts    int        `json:"attempts"`
	Title       string     `json:"title"`        // Extracted title
	Redirected  bool       `json:"redirected"`   // A redirect was followed
	FinalURL    string     `json:"final_url"`    // Final URL after redirects
	Error       string     `json:"error"`        // Failure reason
	SkipReason  string     `json:"skip_reason"`  // Why the late result was skipped
	PublishedAt *time.Time `json:"published_at"` // Publish time
}

type ResponsePageFetchJob struct {
	ID         uint                       `json:"id"`
	SiteName   string                     `json:"site_name"`
	Status     string                     `json:"status"` // running | completed
	IsProgress bool                       `json:"is_progress"`
	Total      int                        `json:"total"`
	Success    int                        `json:"success"`
	Failed     int                        `json:"failed"`
	Skipped    int                        `json:"skipped"`
	Remaining  int                        `json:"remaining"`
	CreatedAt  time.Time                  `json:"created_at"`
	UpdatedAt  time.Time                  `json:"updated_at"`
	Items      []ResponsePageFetchJobItem `json:"items"`
}

func newResponsePageFetchJob(app *core.App, jobID uint) (ResponsePageFetchJob, error) {
	job, items, err := app.Dao().FindPageFetchJob(jobID)
	if err != nil {
		return ResponsePageFetchJob{}, err
	}

	resp := ResponsePageFetchJob{
		ID:         job.ID,
		SiteName:   job.SiteName,
		Status:     job.Status,
		IsProgress: job.Status == entity.PageFetchJobStatusRunning,
		Total:      job.Total,
		Success:    job.Success,
		Failed:     job.Failed,
		Skipped:    job.Skipped,
		CreatedAt:  job.CreatedAt,
		UpdatedAt:  job.UpdatedAt,
		Items:      []ResponsePageFetchJobItem{},
	}
	resp.Remaining = resp.Total - resp.Success - resp.Failed - resp.Skipped
	if resp.Remaining < 0 {
		resp.Remaining = 0
	}

	for _, item := range items {
		resp.Items = append(resp.Items, ResponsePageFetchJobItem{
			ID:          item.ID,
			PageID:      item.PageID,
			PageKey:     item.PageKey,
			SiteName:    item.SiteName,
			Status:      item.Status,
			Attempts:    item.Attempts,
			Title:       item.Title,
			Redirected:  item.Redirected,
			FinalURL:    item.FinalURL,
			Error:       item.Error,
			SkipReason:  item.SkipReason,
			PublishedAt: item.PublishedAt,
		})
	}

	return resp, nil
}

func pageFetchJobNotFound(c *fiber.Ctx, err error) error {
	if err == dao.ErrPageFetchJobNotFound {
		return common.RespError(c, 404, i18n.T("{{name}} not found", Map{"name": "Job"}))
	}
	return common.RespError(c, 500, err.Error())
}

// @Id           GetPageFetchJob
// @Summary      Get Page Fetch Job
// @Description  Get the detail (counts and per-page results) of a page fetch job
// @Tags         Page
// @Security     ApiKeyAuth
// @Param        id  path  int  true  "The job ID"
// @Produce      json
// @Success      200  {object}  ResponsePageFetchJob
// @Failure      404  {object}  Map{msg=string}
// @Failure      500  {object}  Map{msg=string}
// @Router       /pages/fetch/jobs/{id}  [get]
func PageFetchJob(app *core.App, router fiber.Router) {
	router.Get("/pages/fetch/jobs/:id", common.AdminGuard(app, func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")

		resp, err := newResponsePageFetchJob(app, uint(id))
		if err != nil {
			return pageFetchJobNotFound(c, err)
		}
		return common.RespData(c, resp)
	}))
}

// @Id           GetLatestPageFetchJob
// @Summary      Get Latest Page Fetch Job
// @Description  Get the most recent page fetch job for a site scope, used to recover the sidebar view after closing the browser or a server restart
// @Tags         Page
// @Security     ApiKeyAuth
// @Param        site_name  query  string  false  "The site name scope, empty means all sites"
// @Produce      json
// @Success      200  {object}  ResponsePageFetchJob
// @Failure      500  {object}  Map{msg=string}
// @Router       /pages/fetch/jobs/latest  [get]
func PageFetchJobLatest(app *core.App, router fiber.Router) {
	router.Get("/pages/fetch/jobs/latest", common.AdminGuard(app, func(c *fiber.Ctx) error {
		siteName := c.Query("site_name")

		job, err := app.Dao().FindLatestPageFetchJob(siteName)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}
		if job == nil {
			return common.RespData(c, nil)
		}

		resp, err := newResponsePageFetchJob(app, job.ID)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}
		return common.RespData(c, resp)
	}))
}

// @Id           RetryFailedPageFetchJob
// @Summary      Retry Failed Items of a Page Fetch Job
// @Description  Resume a finished page fetch job, retrying only its failed items
// @Tags         Page
// @Security     ApiKeyAuth
// @Param        id  path  int  true  "The job ID"
// @Produce      json
// @Success      200  {object}  ResponsePageFetchJob
// @Failure      404  {object}  Map{msg=string}
// @Failure      500  {object}  Map{msg=string}
// @Router       /pages/fetch/jobs/{id}/retry  [post]
func PageFetchJobRetry(app *core.App, router fiber.Router) {
	router.Post("/pages/fetch/jobs/:id/retry", common.AdminGuard(app, func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")

		job, err := app.Dao().RetryFailedPageFetchJob(uint(id))
		if err != nil {
			return pageFetchJobNotFound(c, err)
		}

		resp, err := newResponsePageFetchJob(app, job.ID)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}
		return common.RespData(c, resp)
	}))
}
