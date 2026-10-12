package dao

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/artalkjs/artalk/v2/internal/entity"
	"github.com/artalkjs/artalk/v2/internal/log"
	"github.com/artalkjs/artalk/v2/internal/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ===============
//  Comment
// ===============

// 获取评论回复链接
func (dao *Dao) GetLinkToReplyByComment(c *entity.Comment, notifyKey ...string) string {
	page := dao.FetchPageForComment(c)
	rawURL := dao.GetPageAccessibleURL(&page)

	// 请求 query
	queryMap := map[string]string{
		"atk_comment": fmt.Sprintf("%d", c.ID),
	}

	// atk_notify_key
	if len(notifyKey) > 0 {
		queryMap["atk_notify_key"] = notifyKey[0]
	}

	return utils.AddQueryToURL(rawURL, queryMap)
}

// ===============
//  Page
// ===============

// 获取可访问链接
func (dao *Dao) GetPageAccessibleURL(p *entity.Page, s ...*entity.Site) string {
	if p.AccessibleURL == "" {
		acURL := p.Key

		// 若 pageKey 为相对路径，生成相对于 site.FirstUrl 配置的 URL
		if !utils.ValidateURL(p.Key) {
			var site *entity.Site
			if len(s) > 0 && s[0] != nil {
				site = s[0]
			} else {
				findSite := dao.FetchSiteForPage(p)
				site = &findSite
			}

			if site != nil {
				u1, e1 := url.Parse(dao.CookSite(site).FirstUrl)
				u2, e2 := url.Parse(p.Key)
				if e1 == nil && e2 == nil {
					acURL = u1.ResolveReference(u2).String()
				}
			}
		}

		p.AccessibleURL = acURL
	}

	return p.AccessibleURL
}

func (dao *Dao) FetchPageFromURL(p *entity.Page) error {
	cookedPage := dao.CookPage(p)
	url := cookedPage.URL

	if url == "" {
		return fmt.Errorf("URL cannot be null")
	}

	// 获取 URL 页面 title
	title, err := GetTitleByURL(url)

	if err == nil && title != "" {
		p.Title = title
	}

	if err := dao.UpdatePage(p); err != nil {
		log.Error("Failed to save in FetchPage")
		return err
	}

	return nil
}

// Page title publish outcomes of a late batch-fetch result.
const (
	PageTitlePublished = "published" // The fetched title was written to the db and cache
	PageTitleUnchanged = "unchanged" // The current title already equals the fetched one, nothing to write
	PageTitleSkipped   = "skipped"   // The page was renamed, moved, deleted or manually re-titled
)

// PublishFetchedPageTitle writes a fetched title only when the page still belongs
// to the original site and neither the key nor the title version (title snapshot)
// were changed after the job started. A stale result is reported as PageTitleSkipped
// and never restores old values or recreates a missing page.
func (dao *Dao) PublishFetchedPageTitle(pageID uint, snapshotKey, snapshotSite, snapshotTitle, fetchedTitle string) (string, error) {
	var page entity.Page
	if err := dao.DB().First(&page, pageID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return PageTitleSkipped, nil // The page was deleted; do not recreate it
		}
		return "", err
	}

	if page.Key != snapshotKey || page.SiteName != snapshotSite || page.Title != snapshotTitle {
		return PageTitleSkipped, nil
	}

	if fetchedTitle == "" || page.Title == fetchedTitle {
		return PageTitleUnchanged, nil
	}

	// Conditional update: confirm the identity and the title version again at write
	// time so a concurrent admin rename/move/re-title cannot be overwritten.
	result := dao.DB().Model(&entity.Page{}).
		Where("id = ? AND site_name = ? AND title = ?", pageID, snapshotSite, snapshotTitle).
		Where(clause.Eq{Column: clause.Column{Name: "key"}, Value: snapshotKey}).
		Update("title", fetchedTitle)
	if result.Error != nil {
		return "", result.Error
	}
	if result.RowsAffected == 0 {
		return PageTitleSkipped, nil
	}

	// Refresh the cache with the published value instead of writing to it blindly.
	var fresh entity.Page
	if err := dao.DB().First(&fresh, pageID).Error; err == nil {
		dao.CacheAction(func(cache *DaoCache) {
			_ = cache.PageCacheSave(&fresh)
		})
	}

	return PageTitlePublished, nil
}

// pageFetchHTTPClient limits how long a single page fetch may block.
// Without the timeout an unreachable host may stall the batch worker forever.
var pageFetchHTTPClient = &http.Client{
	Timeout: 15 * time.Second,
}

// maxPageFetchRedirects bounds html meta-refresh hopping while resolving a title.
const maxPageFetchRedirects = 10

// ErrPageFetchURLEmpty is returned when a page has no accessible URL to fetch.
var ErrPageFetchURLEmpty = errors.New("URL cannot be null")

func GetTitleByURL(rawURL string) (string, error) {
	result, err := FetchPageTitleDetail(context.Background(), rawURL)
	if err != nil {
		return "", err
	}
	return result.Title, nil
}

// PageTitleFetchResult is the detailed outcome of fetching a page title.
type PageTitleFetchResult struct {
	Title     string // Title resolved from the final page
	FinalURL  string // Final URL after following html redirects
	Redirects int    // Number of html redirects followed
}

// FetchPageTitleDetail requests the URL, follows html meta refresh redirects and
// returns the resolved title together with redirect tracing.
func FetchPageTitleDetail(ctx context.Context, rawURL string) (PageTitleFetchResult, error) {
	currentURL := rawURL
	redirects := 0

	for {
		if !utils.ValidateURL(currentURL) {
			log.Error("Invalid URL: " + currentURL)
			return PageTitleFetchResult{FinalURL: currentURL, Redirects: redirects}, fmt.Errorf("invalid URL")
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, currentURL, nil)
		if err != nil {
			return PageTitleFetchResult{FinalURL: currentURL, Redirects: redirects}, err
		}

		// Request the HTML page.
		res, err := pageFetchHTTPClient.Do(req)
		if err != nil {
			log.Error(err)
			return PageTitleFetchResult{FinalURL: currentURL, Redirects: redirects}, err
		}

		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			log.Error(readErr)
			return PageTitleFetchResult{FinalURL: currentURL, Redirects: redirects}, readErr
		}

		if res.StatusCode != http.StatusOK {
			err := fmt.Errorf("status code error: %d '%s' '%s'", res.StatusCode, res.Status, currentURL)
			log.Error(err)
			return PageTitleFetchResult{FinalURL: currentURL, Redirects: redirects}, fmt.Errorf("status code error")
		}

		data := pageExtractFromHTML(body)
		if data.RedirectURL != "" {
			if redirects >= maxPageFetchRedirects {
				return PageTitleFetchResult{FinalURL: currentURL, Redirects: redirects}, fmt.Errorf("too many redirects")
			}
			redirects++
			currentURL = data.RedirectURL
			continue
		}

		return PageTitleFetchResult{
			Title:     data.Title,
			FinalURL: currentURL,
			Redirects: redirects,
		}, nil
	}
}

// the data extracted from a page html
type pageExtractData struct {
	Title       string
	RedirectURL string
}

func pageExtractFromHTML(html []byte) (data pageExtractData) {
	// 读取页面 title
	titleTagReg := regexp.MustCompile(`(?i)<title>(.*?)</title>`)
	titleTagMatch := titleTagReg.FindSubmatch(html)
	if len(titleTagMatch) > 1 {
		data.Title = strings.TrimSpace(string(titleTagMatch[1]))
	}

	// 如果页面有跳转
	redirectTagReg := regexp.MustCompile(`(?i)<meta.*?http-equiv="refresh.*?content="(.*?)".*?>`)
	redirectTagMatch := redirectTagReg.FindSubmatch(html)
	if len(redirectTagMatch) > 1 {
		urlReg := regexp.MustCompile(`https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)`)
		match := urlReg.FindSubmatch(redirectTagMatch[1])
		if len(match) > 0 {
			data.RedirectURL = strings.TrimSpace(string(match[0]))
		}
	}

	return data
}

// ===============
//  Notify
// ===============

func (dao *Dao) NotifySetInitial(n *entity.Notify) error {
	n.IsRead = false
	n.IsEmailed = false
	return dao.DB().Save(n).Error
}

func (dao *Dao) NotifySetRead(n *entity.Notify) error {
	n.IsRead = true
	nowTime := time.Now()
	n.ReadAt = &nowTime
	return dao.DB().Save(n).Error
}

func (dao *Dao) NotifySetEmailed(n *entity.Notify) error {
	n.IsEmailed = true
	nowTime := time.Now()
	n.EmailAt = &nowTime
	return dao.DB().Save(n).Error
}

func (dao *Dao) GetReadLinkByNotify(n *entity.Notify) string {
	c := dao.FetchCommentForNotify(n)

	return dao.GetLinkToReplyByComment(&c, n.Key)
}

// ===============
//	Vote
// ===============

func (dao *Dao) VoteSync() {
	var comments []entity.Comment
	dao.DB().Find(&comments)

	for _, c := range comments {
		voteUp := dao.GetVoteNum(c.ID, string(entity.VoteTypeCommentUp))
		voteDown := dao.GetVoteNum(c.ID, string(entity.VoteTypeCommentDown))
		c.VoteUp = int(voteUp)
		c.VoteDown = int(voteDown)
		dao.UpdateComment(&c)
	}

	var pages []entity.Page
	dao.DB().Find(&pages)

	for _, p := range pages {
		voteUp := dao.GetVoteNum(p.ID, string(entity.VoteTypePageUp))
		voteDown := dao.GetVoteNum(p.ID, string(entity.VoteTypePageDown))
		p.VoteUp = voteUp
		p.VoteDown = voteDown
		dao.UpdatePage(&p)
	}
}
