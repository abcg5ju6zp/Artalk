package dao_test

import (
	"testing"

	"github.com/artalkjs/artalk/v2/internal/dao"
	"github.com/artalkjs/artalk/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishFetchedPageTitle(t *testing.T) {
	app, err := test.NewTestApp()
	require.NoError(t, err)
	defer app.Cleanup()

	d := app.Dao()

	t.Run("publishes when key site and title version are unchanged", func(t *testing.T) {
		page := d.NewPage("/guard/publish.html", "old title", "Site A")
		outcome, err := d.PublishFetchedPageTitle(page.ID, page.Key, page.SiteName, page.Title, "new title")
		require.NoError(t, err)
		assert.Equal(t, dao.PageTitlePublished, outcome)

		refreshed := d.FindPageByID(page.ID)
		assert.Equal(t, "new title", refreshed.Title)
	})

	t.Run("reports unchanged when fetched title equals current title", func(t *testing.T) {
		page := d.NewPage("/guard/same.html", "same title", "Site A")
		outcome, err := d.PublishFetchedPageTitle(page.ID, page.Key, page.SiteName, page.Title, "same title")
		require.NoError(t, err)
		assert.Equal(t, dao.PageTitleUnchanged, outcome)

		assert.Equal(t, "same title", d.FindPageByID(page.ID).Title)
	})

	t.Run("skips when title was manually edited after snapshot", func(t *testing.T) {
		page := d.NewPage("/guard/manual.html", "snapshot title", "Site A")
		page.Title = "manual title"
		require.NoError(t, d.UpdatePage(&page))

		outcome, err := d.PublishFetchedPageTitle(page.ID, page.Key, page.SiteName, "snapshot title", "late title")
		require.NoError(t, err)
		assert.Equal(t, dao.PageTitleSkipped, outcome)

		assert.Equal(t, "manual title", d.FindPageByID(page.ID).Title, "manual title must not be overwritten")
	})

	t.Run("skips when page was renamed after snapshot", func(t *testing.T) {
		page := d.NewPage("/guard/old_key.html", "snapshot title", "Site A")
		page.Key = "/guard/new_key.html"
		require.NoError(t, d.UpdatePage(&page))

		outcome, err := d.PublishFetchedPageTitle(page.ID, "/guard/old_key.html", page.SiteName, "snapshot title", "late title")
		require.NoError(t, err)
		assert.Equal(t, dao.PageTitleSkipped, outcome)

		refreshed := d.FindPageByID(page.ID)
		assert.Equal(t, "/guard/new_key.html", refreshed.Key)
		assert.Equal(t, "snapshot title", refreshed.Title)
	})

	t.Run("skips when page was moved to another site after snapshot", func(t *testing.T) {
		page := d.NewPage("/guard/moved.html", "snapshot title", "Site A")
		page.SiteName = "Site B"
		require.NoError(t, d.UpdatePage(&page))

		outcome, err := d.PublishFetchedPageTitle(page.ID, page.Key, "Site A", "snapshot title", "late title")
		require.NoError(t, err)
		assert.Equal(t, dao.PageTitleSkipped, outcome)

		assert.Equal(t, "snapshot title", d.FindPageByID(page.ID).Title)
	})

	t.Run("skips deleted page without recreating it", func(t *testing.T) {
		page := d.NewPage("/guard/deleted.html", "snapshot title", "Site A")
		pageID := page.ID
		require.NoError(t, d.DelPage(&page))

		outcome, err := d.PublishFetchedPageTitle(pageID, page.Key, page.SiteName, page.Title, "late title")
		require.NoError(t, err)
		assert.Equal(t, dao.PageTitleSkipped, outcome)

		assert.True(t, d.FindPageByID(pageID).IsEmpty(), "page must not be recreated")
	})
}
