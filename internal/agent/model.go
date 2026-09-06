package agent

// RawUpdate - входное сообщение из топика link.raw-updates
type RawUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	Author      string  `json:"author"`
	TgChatIDs   []int64 `json:"tgChatIds"`
}

// ProcessedUpdate - выходное сообщение в топик link.processed-updates
type ProcessedUpdate struct {
	ID          int64   `json:"id"`
	URL         string  `json:"url"`
	Description string  `json:"description"`
	TgChatIDs   []int64 `json:"tgChatIds"`
	Priority    string  `json:"priority"`
}
