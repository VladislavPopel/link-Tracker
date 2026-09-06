package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	httpTimeout      = 30 * time.Second
	summaryMaxLength = 150
	summaryMinLength = 30
)

// Summarizer сокращает длинный текст
type Summarizer interface {
	Summarize(ctx context.Context, text string) (string, error)
}

// StubSummarizer обрезает текст до threshold символов и добавляет "..."
// Используется по умолчанию когда AI API не настроен.
type StubSummarizer struct {
	Threshold int
}

func (s *StubSummarizer) Summarize(_ context.Context, text string) (string, error) {
	runes := []rune(text)
	if len(runes) <= s.Threshold {
		return text, nil
	}
	return string(runes[:s.Threshold]) + "...", nil
}

// AISummarizer вызывает внешний AI API
type AISummarizer struct {
	apiURL     string
	token      string
	httpClient *http.Client
	threshold  int
}

func NewAISummarizer(apiURL, token string, threshold int) *AISummarizer {
	return &AISummarizer{
		apiURL:     apiURL,
		token:      token,
		threshold:  threshold,
		httpClient: &http.Client{Timeout: httpTimeout},
	}
}

type hfRequest struct {
	Inputs     string         `json:"inputs"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type hfResponse []struct {
	SummaryText string `json:"summary_text"`
}

func (s *AISummarizer) Summarize(ctx context.Context, text string) (string, error) {
	prompt := fmt.Sprintf("Summarize the following update in 2-3 sentences:\n\n%s", text)

	payload := hfRequest{
		Inputs: prompt,
		Parameters: map[string]any{
			"max_length": summaryMaxLength,
			"min_length": summaryMinLength,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // не проверяем ошибку

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("AI API returned status %d: %s", resp.StatusCode, raw)
	}

	var result hfResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}

	if len(result) == 0 || result[0].SummaryText == "" {
		// Fallback к заглушке если AI вернул пустой результат
		stub := &StubSummarizer{Threshold: s.threshold}
		return stub.Summarize(ctx, text)
	}

	return result[0].SummaryText, nil
}

// NewSummarizer создаёт нужную реализацию на основе конфигурации
func NewSummarizer(cfg SummarizationConfig) Summarizer {
	if cfg.AIEnabled && cfg.AIURL != "" {
		return NewAISummarizer(cfg.AIURL, cfg.AIToken, cfg.Threshold)
	}
	return &StubSummarizer{Threshold: cfg.Threshold}
}
