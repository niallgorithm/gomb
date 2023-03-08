package gomb

import (
	"context"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// *******************************************
// Pulsar Broker
// *******************************************

// pulsarBroker is a pulsar broker.
type pulsarBroker struct {
	// options is the broker options.
	// comma delimited list of pulsar brokers
	// https://pulsar.apache.org/docs/2.11.x/client-libraries-go/#connection-urls
	Brokers string

	// operationTimeout is the timeout for operations.
	operationTimeout time.Duration

	// connectionTimeout is the timeout for connections.
	connectionTimeout time.Duration

	// options
	options *BrokerOptions

	// client
	client pulsar.Client

	// client options
	clientOptions pulsar.ClientOptions
}

// newPulsarBroker creates a new pulsar broker.
func newPulsarBroker(opts BrokerOptions) (Broker, error) {
	var err error

	// check if broker type is set
	// and if it is valid
	t := opts.BrokerType
	if t == "" {
		return nil, ErrMissingBroker
	}

	// check auth is ok
	if opts.Auth != nil {
		// auth is not required
		if err := opts.Auth.Check(); err != nil {
			return nil, err
		}
	}

	pulsarOpts := pulsar.ClientOptions{
		URL: opts.Address,
	}

	// set auth
	if opts.Auth != nil {
		// TLS or OAuth2 are supported
		if opts.Auth.oauth2Set() {
			// https://pulsar.apache.org/docs/2.11.x/client-libraries-go/#oauth2-authentication
			pulsarOpts.Authentication = pulsar.NewAuthenticationOAuth2(
				opts.Auth.OAuth2.ToMap(),
			)
		} else if opts.Auth.tlsSet() {
			// https://pulsar.apache.org/docs/2.11.x/client-libraries-go/#tls-authentication
			pulsarOpts.Authentication = pulsar.NewAuthenticationTLS(
				opts.Auth.TLS.CertFile,
				opts.Auth.TLS.KeyFile,
			)
			pulsarOpts.TLSTrustCertsFilePath = opts.Auth.TLS.CertFile
		} else {
			// return an error as the auth is not supported
			return nil, &ErrAuth{
				Msg: "Unsupported auth type - pulsar supports TLS and OAuth2",
			}
		}
	}

	// create the client
	client, err := pulsar.NewClient(pulsarOpts)
	if err != nil {
		return nil, err
	}

	// set defaults for timeouts
	if opts.OperationTimeout == 0 {
		opts.OperationTimeout = 30 * time.Second
	}
	if opts.ConnectionTimeout == 0 {
		opts.ConnectionTimeout = 30 * time.Second
	}

	// create the broker
	return &pulsarBroker{
		Brokers:           opts.Address,
		operationTimeout:  opts.OperationTimeout,
		connectionTimeout: opts.ConnectionTimeout,
		options:           &opts,
		client:            client,
		clientOptions:     pulsarOpts,
	}, nil
}

// Connect connects to the broker.
func (b *pulsarBroker) Connect(ctx context.Context) error {
	if b.client == nil {
		// set the client
		client, err := pulsar.NewClient(b.clientOptions)
		if err != nil {
			return ErrConnection
		}
		b.client = client
	}
	return nil
}

// Close closes the broker.
func (b *pulsarBroker) Close() error {
	if b.client != nil {
		b.client.Close()
		b.client = nil
	}
	return nil
}

// AckMessage acknowledges a message.
func (b *pulsarBroker) AckMessage(ctx context.Context, msg BrokerMessage) error {
	// todo: implement
	return nil
}

// Options returns the broker options.
func (b *pulsarBroker) Options() *BrokerOptions {
	return b.options
}

// ************************************************
// * Publisher
// ************************************************

// Publish publishes a message to the broker.
func (b *pulsarBroker) Publish(ctx context.Context, msg BrokerMessage, topics ...string) error {
	for _, topic := range topics {
		// todo: create a producer per topic and reuse it
		// create the producer
		producer, err := b.client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
		if err != nil {
			return err
		}

		// publish the message
		_, err = producer.Send(ctx, &pulsar.ProducerMessage{
			Payload:    msg.Body(),
			Key:        msg.Key(),
			Properties: msg.Headers(),
			EventTime:  msg.Timestamp(),
		})
		if err != nil {
			return err
		}

		// close the producer
		producer.Close()
	}
	return nil
}

// ************************************************
// * Consumer
// ************************************************

// pulsarConsumer is a pulsar consumer.
type pulsarConsumer struct {
	// Topics is the list of topics to consume.
	topics []string

	// ConsumerGroup is the consumer group to consume from.
	consumerGroup string

	// broker
	broker *pulsarBroker

	// consumer
	consumer pulsar.Consumer

	// paused
	paused bool

	mu sync.RWMutex
}

// NewConsumer creates a new consumer.
func (b *pulsarBroker) NewConsumer(opts ConsumerOptions) (Consumer, error) {
	// create the consumer
	return &pulsarConsumer{
		topics:        opts.Topics,
		consumerGroup: opts.ConsumerGroup,
		broker:        b,
	}, nil
}

// Consume consumes messages from the topics.
func (c *pulsarConsumer) Consume(ctx context.Context) (<-chan BrokerMessage, error) {
	var err error
	// create the channel
	ch := make(chan BrokerMessage)

	// create the consumer
	c.consumer, err = c.broker.client.Subscribe(pulsar.ConsumerOptions{
		Topics:            c.topics,
		SubscriptionName:  c.consumerGroup,
		Type:              pulsar.Shared,
		ReceiverQueueSize: 1000,
	})
	if err != nil {
		return nil, err
	}

	// consume messages
	go func() {
		for {
			c.loopUntilUnpaused()
			select {
			case <-ctx.Done():
				return
			default:
				// consume the message
				m, err := c.consumer.Receive(ctx)
				if err != nil {
					return
				}

				// create the broker message
				msg := &Message{}
				msg.SetBody(m.Payload())
				msg.SetKey(m.Key())
				msg.SetHeaders(m.Properties())
				msg.SetTimestamp(m.EventTime())
				msg.SetID(string(m.ID().Serialize()))
				msg.SetGroup(c.consumerGroup)
				msg.SetTopic(m.Topic())
				msg.SetBroker(c.broker)

				// send the message
				ch <- msg
			}
		}
	}()

	return ch, nil
}

// Pause pauses the consumer.
func (c *pulsarConsumer) Pause() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = true
}

// Resume resumes the consumer.
func (c *pulsarConsumer) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
}

// Topics returns the list of topics.
func (c *pulsarConsumer) Topics() []string {
	return c.topics
}

// Close closes the consumer.
func (c *pulsarConsumer) Close() error {
	c.consumer.Close()
	return nil
}

// Connect connects to the broker.
func (c *pulsarConsumer) Connect(ctx context.Context) error {
	return c.broker.Connect(ctx)
}

// ConsumerGroup returns the consumer group.
func (c *pulsarConsumer) ConsumerGroup() string {
	return c.consumerGroup
}

// loopUntilUnpaused is a helper function that will loop until the consumer is unpaused.
func (c *pulsarConsumer) loopUntilUnpaused() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for c.paused {
		// sleep for a bit?
		time.Sleep(time.Millisecond * 100)
	}
}
