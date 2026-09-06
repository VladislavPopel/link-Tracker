package scrapper

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/client"
)

var ErrNoNewEvents = errors.New("no new events")

const PreviewMaxLen = 200

// UpdateInfo содержит детали обнаруженного обновления
type UpdateInfo struct {
	Title     string
	Author    string
	CreatedAt time.Time
	Preview   string
}

// CheckResult - результат одной проверки ссылки
// Если Info == nil - обновлений нет (или это первая проверка для инициализации состояния)
type CheckResult struct {
	UpdatedAt time.Time
	Info      *UpdateInfo
}

// LinkChecker проверяет обновления для одной ссылки
type LinkChecker interface {
	Check(ctx context.Context, url string, since time.Time) (*CheckResult, error)
	Supports(url string) bool
}

func (r *CheckResult) ToLinkUpdate(id int64, url string, chatIDs []int64) client.LinkUpdate {
	update := client.LinkUpdate{
		ID:        id,
		URL:       url,
		TgChatIDs: chatIDs,
	}
	if r.Info != nil {
		update.Description = fmt.Sprintf("%s\n\n%s", r.Info.Title, r.Info.Preview)
		update.Author = r.Info.Author
	}
	return update
}

// GitHub

// GitHubChecker реализует LinkChecker
type GitHubChecker struct {
	inner *client.GitHubClient
}

func NewGitHubChecker(token string) *GitHubChecker {
	return &GitHubChecker{inner: client.NewGitHubClient(token)}
}

func NewGitHubCheckerWithHTTP(token string, httpClient *http.Client) *GitHubChecker {
	return &GitHubChecker{inner: client.NewGitHubClientWithHTTP(token, httpClient)}
}

func (c *GitHubChecker) Supports(url string) bool {
	return strings.HasPrefix(url, "https://github.com/") ||
		strings.HasPrefix(url, "http://github.com/")
}

// Check возвращает детали нового PR/Issue или nil
func (c *GitHubChecker) Check(ctx context.Context, url string, since time.Time) (*CheckResult, error) {
	if since.IsZero() {
		// Первая проверка: инициализируем состояние, уведомление не отправляем
		t, err := c.inner.GetLastUpdated(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("github init state: %w", err)
		}
		return &CheckResult{UpdatedAt: t}, nil
	}

	issue, err := c.inner.GetLatestUpdate(ctx, url, since)
	if err != nil {
		return nil, fmt.Errorf("github get latest update: %w", err)
	}
	if issue == nil {
		return nil, ErrNoNewEvents
	}

	kind := "Issue"
	if issue.PullRequest != nil {
		kind = "PR"
	}

	preview := truncateRunes(stripHTML(issue.Body), PreviewMaxLen)
	return &CheckResult{
		UpdatedAt: issue.UpdatedAt,
		Info: &UpdateInfo{
			Title:     fmt.Sprintf("[%s] %s", kind, issue.Title),
			Author:    issue.User.Login,
			CreatedAt: issue.CreatedAt,
			Preview:   preview,
		},
	}, nil
}

// StackOverflowChecker реализует LinkChecker для вопросов StackOverflow
type StackOverflowChecker struct {
	inner *client.StackOverflowClient
}

func NewStackOverflowChecker(apiKey string) *StackOverflowChecker {
	return &StackOverflowChecker{inner: client.NewStackOverflowClient(apiKey)}
}

func NewStackOverflowCheckerWithHTTP(key string, httpClient *http.Client) *StackOverflowChecker {
	return &StackOverflowChecker{inner: client.NewStackOverflowClientWithHTTP(key, httpClient)}
}

func (c *StackOverflowChecker) Supports(url string) bool {
	return strings.Contains(url, "stackoverflow.com/questions/")
}

// Check возвращает детали нового ответа или nil, если изменений нет
func (c *StackOverflowChecker) Check(ctx context.Context, url string, since time.Time) (*CheckResult, error) {
	if since.IsZero() {
		t, err := c.inner.GetLastUpdated(ctx, url)
		if err != nil {
			return nil, fmt.Errorf("stackoverflow init state: %w", err)
		}
		return &CheckResult{UpdatedAt: t}, nil
	}

	answer, err := c.inner.GetLatestAnswer(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("stackoverflow get latest answer: %w", err)
	}
	if answer == nil {
		return nil, ErrNoNewEvents
	}

	createdAt := time.Unix(answer.CreationDate, 0)
	if !createdAt.After(since) {
		return nil, ErrNoNewEvents
	}

	preview := truncateRunes(stripHTML(answer.Body), PreviewMaxLen)
	return &CheckResult{
		UpdatedAt: createdAt,
		Info: &UpdateInfo{
			Title:     "Новый ответ на вопрос",
			Author:    answer.Owner.DisplayName,
			CreatedAt: createdAt,
			Preview:   preview,
		},
	}, nil
}

// truncateRunes обрезает строку до n рун и добавляет "…" если она была длиннее
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	runes := []rune(s)
	return string(runes[:n]) + "…"
}

func stripHTML(html string) string {
	var b strings.Builder
	b.Grow(len(html))
	inTag := false
	for _, r := range html {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
		case !inTag:
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}
