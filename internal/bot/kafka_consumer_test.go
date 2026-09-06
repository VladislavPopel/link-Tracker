//go:build integration

package bot_test

// Один Kafka-контейнер на весь пакет (TestMain)

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/segmentio/kafka-go"
	kafkamodule "github.com/testcontainers/testcontainers-go/modules/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/bot"
)

var (
	testBroker    string
	kContainer    *kafkamodule.KafkaContainer
	containerOnce sync.Once
	topicCounter  atomic.Int64 // для генерации уникальных имён топиков
)

// TestMain поднимает один Kafka-контейнер для всего пакета
// После прогона всех тестов контейнер останавливается
func TestMain(m *testing.M) {
	ctx := context.Background()

	containerOnce.Do(func() {
		var err error
		kContainer, err = kafkamodule.Run(
			ctx,
			"confluentinc/cp-kafka:7.6.1",
			kafkamodule.WithClusterID("test-cluster-id"),
		)
		if err != nil {
			panic(fmt.Sprintf("start kafka container: %v", err))
		}

		brokers, err := kContainer.Brokers(ctx)
		if err != nil {
			panic(fmt.Sprintf("get kafka brokers: %v", err))
		}
		testBroker = brokers[0]
	})

	code := m.Run()

	if kContainer != nil {
		_ = kContainer.Terminate(context.Background())
	}
	os.Exit(code)
}

// Test helpers

// uniqueTopics возвращает пару (основной топик, DLQ-топик) с уникальными именами
func uniqueTopics() (topic, dlqTopic string) {
	id := topicCounter.Add(1)
	base := fmt.Sprintf("test-topic-%d", id)
	return base, base + "-dlq"
}

// uniqueGroupID возвращает уникальный consumer group ID
func uniqueGroupID() string {
	return fmt.Sprintf("test-group-%d", time.Now().UnixNano())
}

// createTopics создаёт топики через соединение с контроллером кластера
func createTopics(t *testing.T, names ...string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", testBroker)
	if err != nil {
		t.Fatalf("dial kafka: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("get controller: %v", err)
	}

	ctrlConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer ctrlConn.Close()

	configs := make([]kafka.TopicConfig, len(names))
	for i, name := range names {
		configs[i] = kafka.TopicConfig{
			Topic:             name,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}
	}
	if err = ctrlConn.CreateTopics(configs...); err != nil {
		t.Fatalf("create topics %v: %v", names, err)
	}
}

// publish записывает сообщения в топик с опциональным ключом
func publishWithKey(t *testing.T, topic string, key []byte, payloads ...[]byte) {
	t.Helper()

	msgs := make([]kafka.Message, len(payloads))
	for i, p := range payloads {
		msgs[i] = kafka.Message{Key: key, Value: p}
	}

	deadline := time.Now().Add(15 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		w := &kafka.Writer{
			Addr:         kafka.TCP(testBroker),
			Topic:        topic,
			Balancer:     &kafka.RoundRobin{},
			RequiredAcks: kafka.RequireOne,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		lastErr = w.WriteMessages(ctx, msgs...)
		cancel()
		_ = w.Close()

		if lastErr == nil {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}

	t.Fatalf("publish to %q: %v", topic, lastErr)
}

func publish(t *testing.T, topic string, payloads ...[]byte) {
	t.Helper()
	publishWithKey(t, topic, nil, payloads...)
}

// readDLQ читает до n сообщений из DLQ-топика. Возвращает сколько успело прийти
func readDLQ(t *testing.T, dlqTopic string, want int, timeout time.Duration) []kafka.Message {
	t.Helper()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{testBroker},
		Topic:       dlqTopic,
		GroupID:     fmt.Sprintf("dlq-reader-%d", time.Now().UnixNano()),
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var result []kafka.Message
	for len(result) < want {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			break // timeout или нет сообщений
		}
		result = append(result, msg)
	}
	return result
}

// startConsumer запускает консьюмер в фоне и возвращает stop-функцию
func startConsumer(t *testing.T, c *bot.LinkUpdateConsumer) (stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = c.Run(ctx) // при ctx.Done() возвращает nil
	}()

	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop within 5s — possible goroutine leak")
		}
	}
}

// waitFor опрашивает cond() каждые 50 мс до timeout
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// Mocks

// spySender фиксирует все Telegram-сообщения, переданные через Send()
type spySender struct {
	mu   sync.Mutex
	msgs []tgbotapi.MessageConfig
}

func (s *spySender) Send(msg tgbotapi.MessageConfig) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, msg)
	return len(s.msgs), nil
}

func (s *spySender) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

// failSender всегда возвращает ошибку - симулирует недоступность Telegram
// Используется для проверки retry-логики
type failSender struct {
	calls atomic.Int32
}

func (f *failSender) Send(_ tgbotapi.MessageConfig) (int, error) {
	f.calls.Add(1)
	return 0, errors.New("telegram: connection refused")
}

func (f *failSender) Calls() int { return int(f.calls.Load()) }

// Tests

// TestKafkaConsumer_ValidMessage проверяет штатную обработку:
// корректное сообщение - уведомление доставляется в каждый чат из TgChatIDs
func TestKafkaConsumer_ValidMessage(t *testing.T) {
	topic, dlqTopic := uniqueTopics()
	createTopics(t, topic, dlqTopic)

	update := bot.LinkUpdate{
		ID:          1,
		URL:         "https://github.com/user/repo",
		Description: "New commit pushed",
		TgChatIDs:   []int64{100, 200},
	}
	payload, _ := json.Marshal(update)
	publish(t, topic, payload)

	sender := &spySender{}
	consumer := bot.NewLinkUpdateConsumer(
		[]string{testBroker}, topic, uniqueGroupID(), dlqTopic, 3, sender,
	)
	defer consumer.Close()

	stop := startConsumer(t, consumer)
	defer stop()

	// Ожидаем по одному сообщению на каждый chat ID
	if !waitFor(t, 10*time.Second, func() bool { return sender.Count() == 2 }) {
		t.Errorf("want 2 Telegram messages (one per chat), got %d", sender.Count())
	}
}

// TestKafkaConsumer_InvalidJSON_GoesToDLQ проверяет обработку ошибки десериализации:
// невалидный JSON - никаких попыток - сразу в DLQ
func TestKafkaConsumer_InvalidJSON_GoesToDLQ(t *testing.T) {
	topic, dlqTopic := uniqueTopics()
	createTopics(t, topic, dlqTopic)

	publish(t, topic, []byte(`{not: valid json!!!`))

	sender := &spySender{}
	consumer := bot.NewLinkUpdateConsumer(
		[]string{testBroker}, topic, uniqueGroupID(), dlqTopic, 3, sender,
	)
	defer consumer.Close()

	stop := startConsumer(t, consumer)
	time.Sleep(2 * time.Second)
	stop()

	if got := sender.Count(); got != 0 {
		t.Errorf("want 0 Telegram messages for invalid JSON, got %d", got)
	}

	dlqMsgs := readDLQ(t, dlqTopic, 1, 5*time.Second)
	if len(dlqMsgs) == 0 {
		t.Error("want 1 message in DLQ after deserialization error, got 0")
	}

	// Дополнительно: проверяем, что DLQ-конверт содержит оригинальный payload и причину
	var envelope struct {
		Error   string `json:"error"`
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal(dlqMsgs[0].Value, &envelope); err != nil {
		t.Fatalf("unmarshal DLQ envelope: %v", err)
	}
	if envelope.Error == "" {
		t.Error("DLQ envelope should contain non-empty error field")
	}
}

// TestKafkaConsumer_InvalidData_GoesToDLQ проверяет обработку ошибки валидации:
// JSON валиден, но бизнес-правила нарушены (нет URL / нет chat ID) - в DLQ
func TestKafkaConsumer_InvalidData_GoesToDLQ(t *testing.T) {
	topic, dlqTopic := uniqueTopics()
	createTopics(t, topic, dlqTopic)

	// Валидный JSON, но поля url и tgChatIds пусты
	publish(t, topic, []byte(`{"id": 99, "description": "missing required fields"}`))

	sender := &spySender{}
	consumer := bot.NewLinkUpdateConsumer(
		[]string{testBroker}, topic, uniqueGroupID(), dlqTopic, 3, sender,
	)
	defer consumer.Close()

	stop := startConsumer(t, consumer)
	time.Sleep(2 * time.Second)
	stop()

	if got := sender.Count(); got != 0 {
		t.Errorf("want 0 Telegram messages for invalid data, got %d", got)
	}

	dlqMsgs := readDLQ(t, dlqTopic, 1, 5*time.Second)
	if len(dlqMsgs) == 0 {
		t.Error("want 1 message in DLQ after validation error, got 0")
	}
}

// TestKafkaConsumer_HandlerError_RetriesThenDLQ проверяет retry-логику:
// если Telegram временно недоступен, консьюмер повторяет попытку maxRetries раз,
// после чего сообщение уходит в DLQ
func TestKafkaConsumer_HandlerError_RetriesThenDLQ(t *testing.T) {
	topic, dlqTopic := uniqueTopics()
	createTopics(t, topic, dlqTopic)

	update := bot.LinkUpdate{
		ID:        3,
		URL:       "https://github.com/user/repo",
		TgChatIDs: []int64{999},
	}
	payload, _ := json.Marshal(update)
	publish(t, topic, payload)

	const maxRetries = 2
	sender := &failSender{}
	consumer := bot.NewLinkUpdateConsumer(
		[]string{testBroker}, topic, uniqueGroupID(), dlqTopic, maxRetries, sender,
	)
	defer consumer.Close()

	stop := startConsumer(t, consumer)

	time.Sleep(5 * time.Second)
	stop()

	if got := sender.Calls(); got != maxRetries {
		t.Errorf("want exactly %d Send calls (= maxRetries), got %d", maxRetries, got)
	}

	dlqMsgs := readDLQ(t, dlqTopic, 1, 5*time.Second)
	if len(dlqMsgs) == 0 {
		t.Error("want 1 message in DLQ after retries exhausted, got 0")
	}
}

// TestKafkaConsumer_Integration_ScrapperToBot - интеграционный тест,
// проверяющий полный путь: Scrapper (продюсер) - Kafka - Bot (консьюмер) - Telegram.
// Воспроизводит реальный сценарий:
//  1. Scrapper обнаружил обновление ссылки и отправляет событие в Kafka.
//     Ключ сообщения = URL (так делает KafkaMessageSender в scrapper/kafka_sender.go)
//  2. Bot консьюмер читает событие и рассылает Telegram-уведомления подписчикам.
func TestKafkaConsumer_Integration_ScrapperToBot(t *testing.T) {
	topic, dlqTopic := uniqueTopics()
	createTopics(t, topic, dlqTopic)

	// Сторона Scrapper: пишем через продюсер
	update := bot.LinkUpdate{
		ID:          42,
		URL:         "https://github.com/org/repo",
		Description: "PR merged: feat/kafka-notifications",
		TgChatIDs:   []int64{111, 222, 333},
	}
	payload, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("marshal update: %v", err)
	}
	publishWithKey(t, topic, []byte(update.URL), payload)

	// Сторона Bot: консьюмер обрабатывает сообщение
	sender := &spySender{}
	consumer := bot.NewLinkUpdateConsumer(
		[]string{testBroker}, topic, uniqueGroupID(), dlqTopic, 3, sender,
	)
	defer consumer.Close()

	stop := startConsumer(t, consumer)
	defer stop()

	if !waitFor(t, 10*time.Second, func() bool { return sender.Count() == 3 }) {
		t.Errorf("integration: want 3 Telegram messages (one per tgChatId), got %d", sender.Count())
	}
}
