package output

import (
	"time"

	"log"

	"github.com/Shopify/sarama"
)

// A KafkaProducer encapsulates a connection to a Kafka cluster.
type KafkaProducer struct {
	saramaClient *sarama.Client
	saramaProducer *sarama.Producer
	topic string
	incomingMessages <-chan string
}

// Returns an initialized KafkaProducer.
func NewKafkaProducer(msgChan <-chan string, brokers []string, topic string, bufferTime, bufferBytes int) (*KafkaProducer, error) {
	kp := &KafkaProducer{}

	clientConfig := sarama.NewClientConfig()
	client, err := sarama.NewClient("gocollector", brokers, clientConfig)
	if err != nil {
		log.Println("failed to create kafka client", err)
		return nil, err
	}

	producerConfig := sarama.NewProducerConfig()
	producerConfig.Partitioner = sarama.NewRandomPartitioner()
	producerConfig.MaxBufferedBytes = uint32(bufferBytes)
	producerConfig.MaxBufferTime = time.Duration(bufferTime) * time.Millisecond
	producer, err := sarama.NewProducer(client, producerConfig)
	if err != nil {
		log.Println("failed to create kafka producer", err)
		return nil, err
	}

	kp.saramaClient = client
	kp.saramaProducer = producer
	kp.incomingMessages = msgChan
	kp.topic = topic

	go kp.Start()

	log.Println("kafka producer created")
	return kp, nil
}

func (kp *KafkaProducer) Start() {
	for message := range kp.incomingMessages {
		kp.saramaProducer.QueueMessage(kp.topic, nil, sarama.StringEncoder(message))
	}
}
