package agent

import (
	"log/slog"
	"strings"
)

// FilterResult описывает причину отклонения обновления
type FilterResult struct {
	Allowed bool
	Reason  string
}

// Filter применяет все правила фильтрации к входящему обновлению
// Порядок проверок: длина → стоп-слова → исключённые авторы
type Filter struct {
	stopWords       []string
	excludedAuthors map[string]struct{}
	minLength       int
}

// NewFilter создаёт Filter из конфигурации
func NewFilter(cfg FilterConfig) *Filter {
	excluded := make(map[string]struct{}, len(cfg.ExcludedAuthors))
	for _, a := range cfg.ExcludedAuthors {
		excluded[strings.ToLower(a)] = struct{}{}
	}

	stopWords := make([]string, len(cfg.StopWords))
	for i, w := range cfg.StopWords {
		stopWords[i] = strings.ToLower(w)
	}

	return &Filter{
		stopWords:       stopWords,
		excludedAuthors: excluded,
		minLength:       cfg.MinLength,
	}
}

// Allow возвращает FilterResult: разрешено ли обновление к обработке.
func (f *Filter) Allow(u RawUpdate) FilterResult {
	// минимальная длина текста
	if len([]rune(u.Description)) < f.minLength {
		reason := "text too short"
		slog.Debug("update filtered", "id", u.ID, "reason", reason,
			"length", len([]rune(u.Description)), "min", f.minLength)
		return FilterResult{Allowed: false, Reason: reason}
	}

	// стоп-слова
	lower := strings.ToLower(u.Description)
	for _, word := range f.stopWords {
		if strings.Contains(lower, word) {
			reason := "stop-word: " + word
			slog.Debug("update filtered", "id", u.ID, "reason", reason)
			return FilterResult{Allowed: false, Reason: reason}
		}
	}

	// исключённые авторы
	if _, excluded := f.excludedAuthors[strings.ToLower(u.Author)]; excluded {
		reason := "excluded author: " + u.Author
		slog.Debug("update filtered", "id", u.ID, "reason", reason)
		return FilterResult{Allowed: false, Reason: reason}
	}

	return FilterResult{Allowed: true}
}
