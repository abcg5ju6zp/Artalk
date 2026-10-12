package handler

import (
	"github.com/artalkjs/artalk/v2/internal/core"
	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/i18n"
	"github.com/artalkjs/artalk/v2/server/common"
	"github.com/gofiber/fiber/v2"
)

type ResponsePageFetchStatus struct {
	Msg        string `json:"msg"`         // The message of the task status
	IsProgress bool   `json:"is_progress"` // If the task is in progress
	Done       int    `json:"done"`        // The number of pages that reached a final state
	Total      int    `json:"total"`       // The total number of pages

	JobID     uint `json:"job_id"`    // The id of the reported job
	Success   int  `json:"success"`   // Pages whose title was published or already up to date
	Failed    int  `json:"failed"`    // Pages whose fetch failed and may be retried
	Skipped   int  `json:"skipped"`   // Pages skipped because they were renamed, moved, deleted or manually re-titled
	Remaining int  `json:"remaining"` // Pages not yet confirmed
}

// @Id           GetPageFetchStatus
// @Summary      Get Pages Fetch Status
// @Description  Get the status of the latest "fetch all pages" job
// @Tags         Page
// @Security     ApiKeyAuth
// @Param        site_name  query  string  false  "The site scope, defaults to any"
// @Produce      json
// @Success      200  {object}  ResponsePageFetchStatus
// @Router       /pages/fetch/status  [get]
func PageFetchStatus(app *core.App, router fiber.Router) {
	router.Get("/pages/fetch/status", common.AdminGuard(app, func(c *fiber.Ctx) error {
		var p ParamsPageFetchAll
		if isOK, resp := common.ParamsDecode(c, &p); !isOK {
			return resp
		}

		resp := ResponsePageFetchStatus{
			Msg: i18n.T("{{done}} of {{total}} done", Map{"done": 0, "total": 0}),
		}

		job, err := app.PageFetch().Latest(p.SiteName)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}
		if job != nil {
			counts := app.PageFetch().Counts(job.ID)
			done := counts.Succeeded + counts.Failed + counts.Skipped
			resp = ResponsePageFetchStatus{
				Msg:        i18n.T("{{done}} of {{total}} done", Map{"done": done, "total": counts.Total}),
				IsProgress: job.Status == entity.PageFetchJobRunning,
				Done:       done,
				Total:      counts.Total,
				JobID:      job.ID,
				Success:    counts.Succeeded,
				Failed:     counts.Failed,
				Skipped:    counts.Skipped,
				Remaining:  counts.Remaining(),
			}
		}

		return common.RespData(c, resp)
	}))
}
