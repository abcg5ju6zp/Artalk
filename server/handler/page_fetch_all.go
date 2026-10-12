package handler

import (
	"github.com/artalkjs/artalk/v2/internal/core"
	"github.com/artalkjs/artalk/v2/server/common"
	"github.com/gofiber/fiber/v2"
)

type ParamsPageFetchAll struct {
	SiteName string `json:"site_name" query:"site_name" validate:"optional"` // If not empty, only fetch pages of this site
}

// @Id           FetchAllPages
// @Summary      Fetch All Pages Data
// @Description  Fetch the data of all pages; a batch is persisted as a resumable job
// @Tags         Page
// @Security     ApiKeyAuth
// @Param        options  body  ParamsPageFetchAll  true  "The options"
// @Accept       json
// @Produce      json
// @Success      200  {object}  ResponsePageFetchJob
// @Failure      500  {object}  Map{msg=string}
// @Router       /pages/fetch  [post]
func PageFetchAll(app *core.App, router fiber.Router) {
	router.Post("/pages/fetch", common.AdminGuard(app, func(c *fiber.Ctx) error {
		var p ParamsPageFetchAll
		if isOK, resp := common.ParamsDecode(c, &p); !isOK {
			return resp
		}

		// Start a persisted job; a concurrent start of the same scope
		// returns the job that is already executing.
		job, err := app.PageFetch().Start(p.SiteName)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}

		_, items, err := app.PageFetch().Get(job.ID, false)
		if err != nil {
			return common.RespError(c, 500, err.Error())
		}
		return common.RespData(c, buildJobResponse(app.PageFetch(), job, items))
	}))
}
