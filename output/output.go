package output

import (
	"time"

	"log"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

// A KafkaProducer encapsulates a connection to a Kafka cluster.
type KafkaProducer struct {
	saramaClient *sarama.Client
	saramaProducer *sarama.Producer

	registry metrics.Registry
	eventsQd metrics.Counter
	errorsRx metrics.Counter
	errorsChannelEventsRx metrics.Counter
}

// Returns an initialized KafkaProducer.
func NewKafkaProducer(brokers []string, bufferTime, bufferBytes int) (*KafkaProducer, error) {
	client, err := newSaramaClient(brokers)
	if err != nil {
		log.Println("failed to create kafka client", err)
		return nil, err
	}

	producer, err := newSaramaProducer(client, bufferTime, bufferBytes)
	if err != nil {
		log.Println("failed to create kafka producer", err)
		return nil, err
	}

	kp := &KafkaProducer{}
	kp.saramaClient = client
	kp.saramaProducer = producer

	kp.eventsQd = metrics.NewCounter()
	kp.errorsRx = metrics.NewCounter()
	kp.errorsChannelEventsRx = metrics.NewCounter()

	kp.registry = metrics.NewRegistry()
	kp.registry.Register("events.enqueued", kp.eventsQd)
	kp.registry.Register("errors.received", kp.errorsRx)
	kp.registry.Register("errors.channel_events_received", kp.errorsChannelEventsRx)

	log.Println("kafka producer created")
	return kp, nil
}

func newSaramaClient(brokers []string) (*sarama.Client, error) {
	clientConfig := sarama.NewClientConfig()
	client, err := sarama.NewClient("gocollector", brokers, clientConfig)

	if err != nil {
		return nil, err
	}
	return client, nil
}

func newSaramaProducer(client *sarama.Client, bufferTime, bufferBytes int) (*sarama.Producer, error) {
	producerConfig := sarama.NewProducerConfig()
	producerConfig.Partitioner = sarama.NewRandomPartitioner()
	producerConfig.MaxBufferedBytes = uint32(bufferBytes)
	producerConfig.MaxBufferTime = time.Duration(bufferTime) * time.Millisecond
	producer, err := sarama.NewProducer(client, producerConfig)

	if err != nil {
		return nil, err
	}
	return producer, nil
}

func (kp *KafkaProducer) Start(topic string, messages <-chan string) {
	errors := kp.saramaProducer.Errors()
	for {
		select {
		case message := <-messages:
			kp.eventsQd.Inc(1)
			kp.saramaProducer.QueueMessage(topic, nil, sarama.StringEncoder(message))
		case error := <-errors:
			kp.errorsChannelEventsRx.Inc(1)
			if error != nil {
				kp.errorsRx.Inc(1)
				log.Println("Kafka producer error: ", error)
			}
		}
	}
}

// GetStatistics returns an object storing statistics, which supports JSON
// marshalling.
func (kp *KafkaProducer) GetStatistics() (metrics.Registry, error) {
	return kp.registry, nil
}
