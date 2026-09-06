# LinkTracker

**LinkTracker** – Telegram-бот, который отслеживает изменения на веб-страницах и оперативно информирует пользователя о них.


Это шаблон проекта, который вам необходимо взять за основу для разработки своей системы.


В данном файле должна находиться инструкция для ассистента по запуску и настройке бота.
Полезную для разработки проекта информацию вы можете найти в файле [HELP.md.](./HELP.md)


# Инструкция по запуску

Заполнить в файле .env:
- `DATABASE_DSN` — строка подключения к PostgreSQL 
(`postgres://postgres:postgres@localhost:5432/link_tracker?sslmode=disable` по умолчанию) 
- `DATABASE_ACCESS_TYPE` — `SQL` или `ORM` 
(`SQL` по умолчанию)

### 1. Запуск PostgreSQL кластер Kafka из 3 брокеров и Kafka UI:
    `docker compose up -d`
2. Запуск Scrapper

```
$env:DATABASE_DSN="postgres://postgres:postgres@localhost:5433/link_tracker?sslmode=disable"; `
$env:KAFKA_BROKERS="localhost:29092,localhost:29094,localhost:29095"; `
$env:KAFKA_TOPIC="link-updates"; `
$env:NOTIFICATION_TRANSPORT="kafka"; `
go run ./cmd/scrapper/main.go
```

 3. Запуск Bot

```
$env:TELEGRAM_APITOKEN="ваш_токен"; `
$env:KAFKA_BROKERS="localhost:29092,localhost:29094,localhost:29095"; `
$env:KAFKA_TOPIC="link-updates"; `
$env:KAFKA_DLQ_TOPIC="link-updates-dlq"; `
$env:KAFKA_GROUP_ID="bot-consumer"; `
$env:KAFKA_MAX_RETRIES="3"; `
go run ./cmd/bot/main.go
```

# или

2. Создать файл .env в директории проекта
3. Добавить в файл .env строки ``
```
TELEGRAM_APITOKEN="сюда_вставить_токен"
KAFKA_BROKERS=localhost:29092,localhost:29094,localhost:29095
KAFKA_TOPIC=link-updates
KAFKA_DLQ_TOPIC=link-updates-dlq
KAFKA_GROUP_ID=bot-consumer
KAFKA_MAX_RETRIES=3
```
4. Установить пакет godotenv для парсинга файла .env
5. `go run cmd/bot/main.go` и `go run ./cmd/scrapper`

# Запуск тестов

`go test ./...` - запуск всех тестов
    Запуск unit-тестов:
`go test ./internal/application/...`
`go test ./internal/bot/...`

## Только интеграционный тест Scraper → Kafka → Bot
`go test ./internal/bot/... -run TestKafkaConsumer_Integration_ScrapperToBot -v`

# Отчет(ДЗ№6(доп.пункт))

| Параметр | Значение |
|---|---|
| Инструмент | k6 |
| Кол-во параллельных пользователей  | 12 (2 × 6 ядер) |
| Ramp-up | 1 минута (0 - 12 VU) |
| Основная фаза (stage) | 5 минут при 12 VU |
| Итого длительность | 6 минут |
| Объём данных в БД | 100 000 ссылок |
| Кол-во уникальных пользователей в БД | 1 000 |
| Ссылок на пользователя (в среднем) | 100 |
| Соотношение чтение / запись | 99% GET / 1% POST+DELETE |
| Соотношение POST / DELETE внутри записи | 50% / 50% |


## 1. Тест 1 - Без кэширования
 
### 1.1 GET /links
 
| Метрика | Значение |
|---|---|
| Запрос | `GET /links?tgChatId={id}` |
| RPS | 212.3 |
| Среднее время ответа | 0.64 мс |
| p(50) / медиана | 0.15 мс |
| p(90) | 1.43 мс |
| p(95) | 3.19 мс |
| p(99) | 4.91 мс |
| Максимум | 70.6 мс |
| Кол-во вызовов 200 | 76 431 |
| Кол-во вызовов 500 | 0 |
| Кол-во вызовов 502/504 | 0 |
| Наиболее частые ошибки | Отсутствуют |
 
### 1.2 POST /links
 
| Метрика | Значение |
|---|---|
| Запрос | `POST /links` с телом `{tgChatId, link}` |
| RPS | 0.99 |
| Среднее время ответа | 9.42 мс |
| p(50) / медиана | 8.25 мс |
| p(90) | 12.10 мс |
| p(95) | 19.5 мс |
| p(99) | 29.66 мс |
| Максимум | 80.02 мс |
| Кол-во вызовов 200 | 355 |
| Кол-во вызовов 409 (дубликат) | 0 |
| Кол-во вызовов 500 | 0 |
| Кол-во вызовов 502/504 | 0 |
| Наиболее частые ошибки | Отсутствуют |

## 2. Тест 2 - С кэшированием в Valkey
 
### 2.1 GET /links
 
| Метрика | Значение |
|---|---|
| Запрос | `GET /links?tgChatId={id}` |
| RPS | ~207.3 |
| Среднее время ответа | 1.61 мс |
| p(50) / медиана | 0.22 мс |
| p(90) | 2.89 мс |
| p(95) | 4.12 мс |
| p(99) | 16.34 мс |
| Максимум | 518.58 мс |
| Кол-во вызовов 200 | 74 651 |
| Кол-во вызовов 500 | 0 |
| Кол-во вызовов 502/504 | 0 |
| Наиболее частые ошибки | Отсутствуют |
 
### 2.2 POST /links
 
| Метрика | Значение |
|---|---|
| Запрос | `POST /links` с телом `{tgChatId, link}` |
| RPS | ~1.07 |
| Среднее время ответа | 17.32 мс |
| p(50) / медиана | 9.89 мс |
| p(90) | 20.66 мс |
| p(95) | 34.75 мс |
| p(99) | 131.2 мс |
| Максимум | 682.91 мс |
| Кол-во вызовов 200 | 385 |
| Кол-во вызовов 409 (дубликат) | 0 |
| Кол-во вызовов 500 | 0 |
| Кол-во вызовов 502/504 | 0 |
| Наиболее частые ошибки | Отсутствуют |