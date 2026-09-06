package agent

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// KafkaAgentConfig - настройки Kafka для агента
type KafkaAgentConfig struct {
	Brokers       []string `yaml:"brokers"`
	InputTopic    string   `yaml:"input-topic"`
	OutputTopic   string   `yaml:"output-topic"`
	GroupID       string   `yaml:"group-id"`
	InputTopicDLQ string   `yaml:"input-topic-dlq"`
}

// FilterConfig - параметры фильтрации обновлений
type FilterConfig struct {
	StopWords       []string `yaml:"stop-words"`
	ExcludedAuthors []string `yaml:"excluded-authors"`
	MinLength       int      `yaml:"min-length"`
}

// SummarizationConfig - параметры суммаризации
type SummarizationConfig struct {
	// Threshold - порог длины текста
	Threshold int `yaml:"threshold"`

	AIEnabled bool   `yaml:"ai-enabled"`
	AIURL     string `yaml:"ai-url"`
	AIToken   string `yaml:"ai-token"`
}

// Config - конфигурация сервиса
type Config struct {
	Kafka         KafkaAgentConfig    `yaml:"kafka"`
	Filtering     FilterConfig        `yaml:"filtering"`
	Summarization SummarizationConfig `yaml:"summarization"`
}

// LoadConfig читает конфигурацию из YAML-файла
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}

	cfg := &Config{}
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config file %q: %w", path, err)
	}

	if err = cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return errors.New("kafka.brokers must not be empty")
	}
	if c.Kafka.InputTopic == "" {
		return errors.New("kafka.input-topic must not be empty")
	}
	if c.Kafka.OutputTopic == "" {
		return errors.New("kafka.output-topic must not be empty")
	}
	if c.Kafka.GroupID == "" {
		return errors.New("kafka.group-id must not be empty")
	}
	if c.Filtering.MinLength < 0 {
		return errors.New("filtering.min-length must be >= 0")
	}
	if c.Summarization.Threshold < 0 {
		return errors.New("summarization.threshold must be >= 0")
	}
	return nil
}
