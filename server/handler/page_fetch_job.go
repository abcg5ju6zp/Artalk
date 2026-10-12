package handler

import (
	"time"

	"github.com/artalkjs/artalk/v2/internal/core"
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/i18n"
	"github.com/artalkjs/artalk/v2/internal/pagefetch"
	"github.com/artalkjs/artalk/v2/server/common"
	"github.com/gofiber/fiber/v2"
)

type ResponsePageFetchJobItem struct {
	ID           uint       `json:"id"`
	PageID       uint       `json:"page_id"`
	Key          string     `json:"key"`
	SiteName     string     `json:"site_name"`
	Status       string     `json:"status"` // pending | fetching | succeeded | failed | skipped
	Attempts     int        `json:"attempts"`
	Redirects    int        `json:"redirects"`
	FinalURL     string     `json:"final_url"`
	FetchedTitle string     `json:"fetched_title"`
	Message      string     `json:"message"`
	PublishedAt  *time.Time `json:"published_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type ResponsePageFetchJob struct {
	ID        uint      `json:"id"`
	SiteName  string    `json:"site_name"`
	Status    string    `json:"status"` // running | completed
	Total     int       `json:"total"`
	Success   int       `json:"success"`
	Failed    int       `json:"failed"`
	Skipped   int       `json:"skipped"`
	Remaining int       `json:"remaining"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	Items []ResponsePageFetchJobItem `json:"items,omitempty"`
}

func buildJobItemResponse(item entity.PageFetchJobItem) ResponsePageFetchJobItem {
	return ResponsePageFetchJobItem{
		ID:           item.ID,
		PageID:       item.PageID,
		Key:          item.PageKey,
		SiteName:     item.SiteName,
		Status:       item.Status,
		Attempts:     item.Attempts,
		Redirects:    item.Redirects,
		FinalURL:     item.FinalURL,
		FetchedTitle: item.FetchedTitle,
		Message:      item.Message,
		PublishedAt:  item.PublishedAt,
		UpdatedAt:    item.UpdatedAt,
	}
}

func buildJobResponse(m *pagefetch.Manager, job *entity.PageFetchJob, items []entity.PageFetchJobItem) ResponsePageFetchJob {
	counts := m.Counts(job.ID)
	resp := ResponsePageFetchJob{
		ID:        job.ID,
		SiteName:  job.SiteName,
		Status:    job.Status,
		Total:     counts.Total,
		Success:   counts.Succeeded,
		Failed:    counts.Failed,
		Skipped:   counts.Skipped,
		Remaining: counts.Remaining(),
		CreatedAt: job.CreatedAt,
		UpdatedAt: job.UpdatedAt,
	}
	for _, item := range items {
		resp.Items = append(resp.Items, buildJobItemResponse(item))
	}
	return resp
}

type ParamsPageFetchJobList struct {
	SiteName string `json:"site_name" query:"site_name" validate:"optional"`
}

// @Id           ListPageFetchJobs
// @Summary      List Page Fetch Jobs
// @Description  List persisted "fetch all pages" jobs
// @Tags         Page
// @Security     ApiKeyAuth
// @Produce      json
// @Success      200  {object}  []ResponsePageFetchJob
// @Router       /pages/fetch/jobs  [get]
func PageFetchJobList(app *core.App, router fiber.Router) {
	router.Get("/pages/fetch/jobs", common.AdminGuard(app, func(c *fiber.Ctx) error {
		var p ParamsPageFetchJobList
		if isOK, resp := common.ParamsDecode(c, &p); !isOK {
			return resp
		}

		jobs, err := app.PageFetch().List(p.SiteName, 20)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}

		result := make([]ResponsePageFetchJob, 0, len(jobs))
		for i := range jobs {
			result = append(result, buildJobResponse(app.PageFetch(), &jobs[i], nil))
		}
		return common.RespData(c, result)
	}))
}

// @Id           GetPageFetchJob
// @Summary      Get Page Fetch Job
// @Description  Get a persisted "fetch all pages" job with per-page results
// @Tags         Page
// @Security     ApiKeyAuth
// @Produce      json
// @Param        id  path  int  true  "The job id"
// @Success      200  {object}  ResponsePageFetchJob
// @Failure      404  {object}  Map{msg=string}
// @Router       /pages/fetch/jobs/{id}  [get]
func PageFetchJobGet(app *core.App, router fiber.Router) {
	router.Get("/pages/fetch/jobs/:id", common.AdminGuard(app, func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")

		job, items, err := app.PageFetch().Get(uint(id), true)
		if err == pagefetch.ErrJobNotFound {
			return common.RespError(c, 404, i18n.T("{{name}} not found", Map{"name": i18n.T("Page fetch job")}))
		}
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}

		return common.RespData(c, buildJobResponse(app.PageFetch(), job, items))
	}))
}

// @Id           RetryPageFetchJob
// @Summary      Retry Failed Pages Of A Fetch Job
// @Description  Retry only the failed pages of a finished "fetch all pages" job
// @Tags         Page
// @Security     ApiKeyAuth
// @Produce      json
// @Param        id  path  int  true  "The job id"
// @Success      200  {object}  ResponsePageFetchJob
// @Failure      404  {object}  Map{msg=string}
// @Failure      400  {object}  Map{msg=string}
// @Router       /pages/fetch/jobs/{id}/retry  [post]
func PageFetchJobRetry(app *core.App, router fiber.Router) {
	router.Post("/pages/fetch/jobs/:id/retry", common.AdminGuard(app, func(c *fiber.Ctx) error {
		id, _ := c.ParamsInt("id")

		job, err := app.PageFetch().RetryFailed(uint(id))
		if err == pagefetch.ErrJobNotFound {
			return common.RespError(c, 404, i18n.T("{{name}} not found", Map{"name": i18n.T("Page fetch job")}))
		}
		if err == pagefetch.ErrNoFailedItems {
			return common.RespError(c, 400, i18n.T("No failed pages to retry"))
		}
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}

		_, items, err := app.PageFetch().Get(job.ID, true)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}
		return common.RespData(c, buildJobResponse(app.PageFetch(), job, items))
	}))
}
