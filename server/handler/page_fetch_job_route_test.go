package handler_test

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// In fiber v2 the first registered route at a path segment wins, so the
// static "latest" route has to be registered before ":id" (see server.admin).
func TestPageFetchJobRoutePriority(t *testing.T) {
	app := fiber.New()
	app.Get("/pages/fetch/jobs/latest", func(c *fiber.Ctx) error { return c.SendString("latest") })
	app.Get("/pages/fetch/jobs/:id", func(c *fiber.Ctx) error { return c.SendString("id:" + c.Params("id")) })
	app.Post("/pages/fetch/jobs/:id/retry", func(c *fiber.Ctx) error { return c.SendString("retry:" + c.Params("id")) })

	get := func(path string) string {
		resp, err := app.Test(httptest.NewRequest("GET", path, nil))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}

	if got := get("/pages/fetch/jobs/latest"); got != "latest" {
		t.Fatalf("expected latest, got %q", got)
	}
	if got := get("/pages/fetch/jobs/42"); got != "id:42" {
		t.Fatalf("expected id:42, got %q", got)
	}
}
