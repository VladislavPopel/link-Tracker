//go:build integration

package agent_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent"
)

// startKafka запускает один брокер Kafka через Testcontainers
func startKafka(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	kc, err := kafkacontainer.Run(ctx, "confluentinc/cp-kafka:7.4.0",
		kafkacontainer.WithClusterID("test-cluster"),
	)
	require.NoError(t, err, "start kafka container")

	t.Cleanup(func() {
		if termErr := testcontainers.TerminateContainer(kc); termErr != nil {
			t.Logf("terminate kafka container: %v", termErr)
		}
	})

	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, brokers)
	return brokers[0]
}

// createTopic явно создаёт топик через AdminClient
func createTopic(t *testing.T, broker, topic string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", broker)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck

	controller, err := conn.Controller()
	require.NoError(t, err)

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	require.NoError(t, err)
	defer controllerConn.Close() //nolint:errcheck

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1, // один брокер в Testcontainers
	})
	require.NoError(t, err, "create topic %s", topic)
}

// makeWriter создаёт writer с RequireOne - совместим с одним брокером
func makeWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		RequiredAcks: kafka.RequireOne, // важно: не RequireAll для single-broker
	}
}

// makeOutputReader создаёт reader для проверки output-топика.
// StartOffset=FirstOffset - читаем с самого начала, чтобы не пропустить сообщения.
func makeOutputReader(broker, topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     group,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
}

// readWithTimeout ждёт одно сообщение из reader'а с таймаутом.
func readWithTimeout(t *testing.T, reader *kafka.Reader, timeout time.Duration) (kafka.Message, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	msg, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Logf("readWithTimeout: %v", err)
		return kafka.Message{}, false
	}
	return msg, true
}

// buildProcessor собирает Processor с тестовой конфигурацией.
func buildProcessor(broker, outputTopic string) *agent.Processor {
	filter := agent.NewFilter(agent.FilterConfig{
		StopWords:       []string{"spam"},
		ExcludedAuthors: []string{"bot-user"},
		MinLength:       10,
	})
	summarizer := &agent.StubSummarizer{Threshold: 100}
	writer := makeWriter(broker, outputTopic)
	return agent.NewProcessor(filter, summarizer, writer, 100)
}

// корректное сообщение обрабатывается и появляется в output-топике
func TestIntegration_ValidMessage(t *testing.T) {
	broker := startKafka(t)

	inputTopic := "link.raw-updates-valid"
	outputTopic := "link.processed-updates-valid"

	createTopic(t, broker, inputTopic)
	createTopic(t, broker, outputTopic)

	// Шаг 1: запускаем output reader ДО старта consumer,
	// чтобы не пропустить сообщение (StartOffset=FirstOffset гарантирует чтение с начала)
	outputReader := makeOutputReader(broker, outputTopic, "test-valid-output")
	defer outputReader.Close() //nolint:errcheck

	// Шаг 2: публикуем входное сообщение
	inputWriter := makeWriter(broker, inputTopic)
	defer inputWriter.Close() //nolint:errcheck

	raw := agent.RawUpdate{
		ID:          42,
		URL:         "https://github.com/test/repo",
		Description: "Fixed a critical bug in the authentication module",
		Author:      "alice",
		TgChatIDs:   []int64{111, 222},
	}
	body, err := json.Marshal(raw)
	require.NoError(t, err)

	ctx := context.Background()
	err = inputWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", raw.ID)),
		Value: body,
	})
	require.NoError(t, err, "опубликовать сообщение в input-топик")

	// Шаг 3: запускаем consumer
	kafkaCfg := agent.KafkaAgentConfig{
		Brokers:     []string{broker},
		InputTopic:  inputTopic,
		OutputTopic: outputTopic,
		GroupID:     "test-valid-consumer",
	}
	processor := buildProcessor(broker, outputTopic)
	consumer := agent.NewConsumer(kafkaCfg, processor)
	defer consumer.Close() //nolint:errcheck

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()

	go func() {
		_ = consumer.Run(consumerCtx)
	}()

	// Шаг 4: ждём сообщение в output-топике
	msg, ok := readWithTimeout(t, outputReader, 30*time.Second)
	require.True(t, ok, "сообщение должно появиться в output-топике")

	var processed agent.ProcessedUpdate
	err = json.Unmarshal(msg.Value, &processed)
	require.NoError(t, err, "processed update должен корректно десериализоваться")

	assert.Equal(t, raw.ID, processed.ID)
	assert.Equal(t, raw.URL, processed.URL)
	assert.Equal(t, raw.TgChatIDs, processed.TgChatIDs)
	assert.NotEmpty(t, processed.Description)
	assert.Equal(t, "HIGH", processed.Priority)
}

// TC-1.2: некорректный формат сообщения не роняет сервис,
// следующее валидное сообщение обрабатывается нормально
func TestIntegration_InvalidMessage(t *testing.T) {
	broker := startKafka(t)

	inputTopic := "link.raw-updates-invalid"
	outputTopic := "link.processed-updates-invalid"

	createTopic(t, broker, inputTopic)
	createTopic(t, broker, outputTopic)

	outputReader := makeOutputReader(broker, outputTopic, "test-invalid-output")
	defer outputReader.Close() //nolint:errcheck

	inputWriter := makeWriter(broker, inputTopic)
	defer inputWriter.Close() //nolint:errcheck

	ctx := context.Background()

	// Шаг 1: публикуем невалидный JSON
	err := inputWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte("bad"),
		Value: []byte(`{not valid json`),
	})
	require.NoError(t, err)

	// Шаг 2: следом публикуем валидное сообщение
	valid := agent.RawUpdate{
		ID:          99,
		URL:         "https://github.com/test/repo",
		Description: "Valid update after invalid message",
		Author:      "bob",
		TgChatIDs:   []int64{333},
	}
	body, err := json.Marshal(valid)
	require.NoError(t, err)

	err = inputWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte("99"),
		Value: body,
	})
	require.NoError(t, err)

	// Шаг 3: запускаем consumer
	kafkaCfg := agent.KafkaAgentConfig{
		Brokers:     []string{broker},
		InputTopic:  inputTopic,
		OutputTopic: outputTopic,
		GroupID:     "test-invalid-consumer",
	}
	processor := buildProcessor(broker, outputTopic)
	consumer := agent.NewConsumer(kafkaCfg, processor)
	defer consumer.Close() //nolint:errcheck

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()

	go func() {
		_ = consumer.Run(consumerCtx)
	}()

	// Шаг 4: только валидное сообщение должно появиться в output
	msg, ok := readWithTimeout(t, outputReader, 30*time.Second)
	require.True(t, ok, "валидное сообщение должно быть обработано несмотря на предыдущее невалидное")

	var processed agent.ProcessedUpdate
	err = json.Unmarshal(msg.Value, &processed)
	require.NoError(t, err)
	assert.Equal(t, int64(99), processed.ID)
	assert.Equal(t, "https://github.com/test/repo", processed.URL)
}
