package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/jsonschema"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

var logger slog.Logger

func loggerInit() {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger = *slog.New(handler)
}

func main() {
	loggerInit()
	data, err := os.ReadFile("cmd/config_cloud.yml")
	if err != nil {
		logger.Warn(err.Error())
		return
	}

	var config map[string]any

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		logger.Error(err.Error())
		return
	}
	config_producer := config["producer"].(map[string]any)
	if _, ok := config["schema_registry_url"]; !ok {
		log.Fatalf("Не могу прочитать %s\n", "schema_registry_url")
	}
	url := config["schema_registry_url"].(string)
	schema_config := schemaregistry.NewConfig(url)
	schema_config.BasicAuthCredentialsSource = "SASL_INHERIT"
	if _, ok := config_producer["sasl.username"]; !ok {
		log.Fatalf("Не могу прочитать %s\n", "producer sasl.username")
	}
	schema_config.SaslUsername = config_producer["sasl.username"].(string)
	schema_config.SaslPassword = config_producer["sasl.password"].(string)
	schema_config.SaslMechanism = "SCRAM-SHA-512"
	schema_config.SslCaLocation = config_producer["ssl.ca.location"].(string)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	messages := make(chan Message, 100)

	defer func() {
		stop()
		close(messages)
	}()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go runMessageGenerator(ctx, wg, messages, (1 * time.Second))
	go StartProducer(ctx, wg, config["topic"].(string), config["producer"].(map[string]any), schema_config, messages)
	go StartConsumer(ctx, wg, config["topic"].(string), config["consumer"].(map[string]any), schema_config)
	wg.Wait()
}

type Message struct {
	Uuid    string `json:"uuid"`
	From    string `json:"from"`
	To      string `json:"to"`
	Content string `json:"content"`
}

func StartConsumer(ctx context.Context, wg *sync.WaitGroup, topic string, cfg map[string]any, schema_config *schemaregistry.Config) {
	var cfgMap kafka.ConfigMap = kafka.ConfigMap{}

	for k, v := range cfg {
		cfgMap.SetKey(k, v)
	}
	cfgMap.SetKey("group.id", cfg["group.id"].(string))
	client, err := schemaregistry.NewClient(schema_config)

	if err != nil {
		log.Fatalf("Failed to create schema registry client: %s\n", err)
	}
	deser_config := jsonschema.NewDeserializerConfig()
	deser, err := jsonschema.NewDeserializer(client, serde.ValueSerde, deser_config)

	if err != nil {
		log.Fatalf("Failed to create deserializer: %s\n", err)
	}

	// Создание нового consumer
	c, err := kafka.NewConsumer(&cfgMap)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s\n", err)
	}
	defer func() {
		c.Close()
		wg.Done()
	}()

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s\n", err)
	}

	logger.Info("Consumer started and subscribed to topic", slog.Any("consumer", c), slog.String("topic", topic))

	// Чтение сообщений
	for {
		select {
		case <-ctx.Done():
			logger.Info("Получен сигнал выхода, остановка консьюмера...")
			return
		default:
			var message Message
			msg, err := c.ReadMessage(3 * time.Second)

			if err != nil {
				if err.(kafka.Error).IsTimeout() {
					logger.Debug("Timeout on read message", slog.Any("error", err))
				} else {
					logger.Error("Error while reading message", slog.Any("error", err))
				}
				continue
			}
			err = deser.DeserializeInto(*msg.TopicPartition.Topic, msg.Value, &message)
			if err != nil {
				logger.Error("Failed to deserialize payload", slog.Any("error", err))
			} else {
				logger.Info("Received message", slog.Any("value", message))
			}
		}

	}
}

func StartProducer(ctx context.Context, wg *sync.WaitGroup, topic string, cfg map[string]any, schema_config *schemaregistry.Config, messagesCh <-chan Message) {
	var cfgMap kafka.ConfigMap = kafka.ConfigMap{}
	deliveryChan := make(chan kafka.Event)

	for k, v := range cfg {
		cfgMap.SetKey(k, v)
	}
	p, err := kafka.NewProducer(&cfgMap)
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	defer func() {
		p.Close()
		close(deliveryChan)
		wg.Done()
	}()

	logger.Info("Created Producer", slog.Any("producer", p))
	client, err := schemaregistry.NewClient(schema_config)
	if err != nil {
		log.Fatalf("Failed to create schema registry client: %s\n", err)
	}

	serializer_config := jsonschema.NewSerializerConfig()
	ser, err := jsonschema.NewSerializer(client, serde.ValueSerde, serializer_config)
	if err != nil {
		log.Fatalf("Failed to create serializer: %s\n", err)
	}
	for {
		select {
		case <-ctx.Done():
			logger.Info("Получен сигнал выхода, остановка продьюсера...")
			return
		case msg := <-messagesCh:
			payload, err := ser.Serialize(topic, &msg)
			if err != nil {
				logger.Error("Failed to serialize payload", slog.Any("error", err))
			}

			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(msg.Uuid),
				Value:          payload,
			}, deliveryChan)
			if err != nil {
				logger.Error("Produce failed", slog.Any("error", err))
			}

			e := <-deliveryChan
			m := e.(*kafka.Message)

			if m.TopicPartition.Error != nil {
				logger.Error("Delivery failed", slog.Any("error", m.TopicPartition.Error))
			} else {
				logger.Info("Delivered message ",
					slog.String("topic", *m.TopicPartition.Topic),
					slog.Int("partition", int(m.TopicPartition.Partition)),
					slog.Any("offset", m.TopicPartition.Offset))
			}
		}

	}

}

func runMessageGenerator(ctx context.Context, wg *sync.WaitGroup, messages chan Message, interval time.Duration) {
	timer := time.NewTicker(interval)
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			logger.Info("Получен сигнал выхода, остановка генератора сообщений...")
			return

		case <-timer.C:
			messages <- Message{
				Uuid:    uuid.New().String(),
				From:    "Pipa",
				To:      "Goga",
				Content: "Hello! Again?!",
			}
		}
	}
}
