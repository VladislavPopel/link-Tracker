package agent_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent"
)

const threshold = 50

func newStub() *agent.StubSummarizer {
	return &agent.StubSummarizer{Threshold: threshold}
}

// длинный текст суммаризируется
func TestStubSummarizer_LongText(t *testing.T) {
	s := newStub()
	longText := strings.Repeat("a", threshold+100) // заведомо длиннее порога

	result, err := s.Summarize(context.Background(), longText)

	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(result, "..."), "суммаризированный текст должен оканчиваться на '...'")
	assert.Less(t, len([]rune(result)), len([]rune(longText)), "результат должен быть короче исходного")

	// Длина без "..." не превышает threshold
	trimmed := strings.TrimSuffix(result, "...")
	assert.LessOrEqual(t, len([]rune(trimmed)), threshold)
}

// короткий текст передаётся без изменений
func TestStubSummarizer_ShortText(t *testing.T) {
	s := newStub()
	shortText := "Short update text." // намного меньше threshold

	result, err := s.Summarize(context.Background(), shortText)

	require.NoError(t, err)
	assert.Equal(t, shortText, result, "короткий текст не должен изменяться")
	assert.False(t, strings.HasSuffix(result, "..."), "короткий текст не должен содержать '...'")
}

// Граничный случай: текст ровно в threshold рун - не суммаризируется
func TestStubSummarizer_ExactThreshold(t *testing.T) {
	s := newStub()
	exactText := strings.Repeat("x", threshold)

	result, err := s.Summarize(context.Background(), exactText)

	require.NoError(t, err)
	assert.Equal(t, exactText, result, "текст ровно в threshold символов не должен изменяться")
}

// Граничный случай: текст на 1 руну длиннее threshold - суммаризируется
func TestStubSummarizer_OneOverThreshold(t *testing.T) {
	s := newStub()
	text := strings.Repeat("x", threshold+1)

	result, err := s.Summarize(context.Background(), text)

	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(result, "..."))
}

// Суммаризация корректно работает с Unicode
func TestStubSummarizer_Unicode(t *testing.T) {
	s := &agent.StubSummarizer{Threshold: 10}
	text := "Это длинное обновление на русском языке которое нужно сократить"

	result, err := s.Summarize(context.Background(), text)

	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(result, "..."))
	// Проверяем что обрезка по рунам, а не байтам
	trimmed := []rune(strings.TrimSuffix(result, "..."))
	assert.LessOrEqual(t, len(trimmed), 10)
}

// NewSummarizer возвращает StubSummarizer когда AI не включён
func TestNewSummarizer_ReturnsStubWhenAIDisabled(t *testing.T) {
	cfg := agent.SummarizationConfig{
		Threshold: 100,
		AIEnabled: false,
	}
	s := agent.NewSummarizer(cfg)

	text := strings.Repeat("a", 200)
	result, err := s.Summarize(context.Background(), text)

	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(result, "..."))
}
