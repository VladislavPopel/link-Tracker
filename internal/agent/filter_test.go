package agent_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent"
)

func newTestFilter() *agent.Filter {
	return agent.NewFilter(agent.FilterConfig{
		StopWords:       []string{"spam", "ads", "promo"},
		ExcludedAuthors: []string{"bot-user", "spammer"},
		MinLength:       20,
	})
}

// фильтрация по стоп-слову
func TestFilter_StopWord(t *testing.T) {
	f := newTestFilter()

	tests := []struct {
		name        string
		description string
	}{
		{"exact match", "This is a spam message for testing purposes"},
		{"uppercase", "This is a SPAM message for testing purposes"},
		{"mixed case", "Buy now with our Ads promotion today here"},
		{"word promo", "Special promo offer just for you this week"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := f.Allow(agent.RawUpdate{
				ID:          1,
				Description: tt.description,
				Author:      "normal-user",
				TgChatIDs:   []int64{1},
			})
			assert.False(t, result.Allowed, "обновление со стоп-словом должно быть отфильтровано")
			assert.NotEmpty(t, result.Reason)
		})
	}
}

// фильтрация по исключённому автору
func TestFilter_ExcludedAuthor(t *testing.T) {
	f := newTestFilter()

	tests := []struct {
		name   string
		author string
	}{
		{"exact", "bot-user"},
		{"uppercase", "BOT-USER"},
		{"mixed", "Bot-User"},
		{"second excluded", "spammer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := f.Allow(agent.RawUpdate{
				ID:          2,
				Description: "This is a perfectly valid update text long enough",
				Author:      tt.author,
				TgChatIDs:   []int64{1},
			})
			assert.False(t, result.Allowed, "обновление от исключённого автора должно быть отфильтровано")
			assert.NotEmpty(t, result.Reason)
		})
	}
}

// фильтрация по минимальной длине
func TestFilter_MinLength(t *testing.T) {
	f := newTestFilter()

	tests := []struct {
		name        string
		description string
	}{
		{"empty string", ""},
		{"too short", "short"},
		{"exactly one less", "nineteen characters"}, // 19 рун
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := f.Allow(agent.RawUpdate{
				ID:          3,
				Description: tt.description,
				Author:      "normal-user",
				TgChatIDs:   []int64{1},
			})
			assert.False(t, result.Allowed, "короткое обновление должно быть отфильтровано")
			assert.NotEmpty(t, result.Reason)
		})
	}
}

// валидное обновление проходит все фильтры
func TestFilter_ValidUpdate(t *testing.T) {
	f := newTestFilter()

	tests := []struct {
		name        string
		description string
		author      string
	}{
		{
			"normal update",
			"Fixed a critical bug in the authentication module",
			"alice",
		},
		{
			"exactly min-length",
			"exactly twenty chars!", // 21 руна - проходит
			"bob",
		},
		{
			"long text without stop words",
			"This is a detailed update about a new feature that has been implemented in the repository",
			"charlie",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := f.Allow(agent.RawUpdate{
				ID:          4,
				Description: tt.description,
				Author:      tt.author,
				TgChatIDs:   []int64{1, 2},
			})
			assert.True(t, result.Allowed, "валидное обновление должно пройти фильтрацию")
			assert.Empty(t, result.Reason)
		})
	}
}

// Граничный случай: MinLength=0 пропускает всё по длине
func TestFilter_ZeroMinLength(t *testing.T) {
	f := agent.NewFilter(agent.FilterConfig{
		StopWords:       []string{},
		ExcludedAuthors: []string{},
		MinLength:       0,
	})

	result := f.Allow(agent.RawUpdate{
		ID:          5,
		Description: "",
		Author:      "user",
		TgChatIDs:   []int64{1},
	})
	assert.True(t, result.Allowed)
}
