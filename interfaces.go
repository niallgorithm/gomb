package gomb

import (
	"context"
	"time"
)

// ************************************************
// * BrokerMessage
// ************************************************

// BrokerMessage is the interface that all messages must implement.
// This represents a message that is consumed or published by a Broker.
type BrokerMessage interface {
	// Ack acknowledges the message. This should be called after the
	// message has been successfully processed.
	// Or your could use the Ack() the message and Republish() the message
	Ack(ctx context.Context) error

	// Republish republishes the message. This should be called if the
	// message has not been successfully processed.
	// This will put the message back on the queue to be processed again as a new message.
	Republish(ctx context.Context) error

	// Publish publishes the message to the given topics.
	Publish(ctx context.Context, topics ...string) error

	// Body returns the message body.
	Body() []byte

	// Headers returns the message headers.
	// These are:
	// - Kafka: Headers
	// - Pulsar: Properties
	// - Redis Streams: Fields
	Headers() map[string]string

	// ID returns the message ID.
	// This is:
	// - Kafka: Offset
	// - Pulsar: BrokerMessageID
	// - Redis Streams: ID
	ID() string

	// Key returns the message key.
	// This is:
	// - Kafka: Key
	// - Pulsar: Key
	// - Redis Streams: No equivalent (returns empty string)
	Key() string

	// Timestamp returns the message timestamp.
	// This is:
	// - Kafka: Timestamp
	// - Pulsar: PublishTime
	// - Redis Streams: No equivalent (returns 0)
	Timestamp() time.Time

	// Topic returns the message topic.
	// This is:
	// - Kafka: Topic
	// - Pulsar: Topic
	// - Redis Streams: Channel
	Topic() string

	// Group returns the message group.
	// This is:
	// - Kafka: No equivalent (returns empty string)
	// - Pulsar: No equivalent (returns empty string)
	// - Redis Streams: Group
	Group() string

	// ConsumerGroup returns the message consumer group.
	// This is:
	// - Kafka: ConsumerGroup
	// - Pulsar: ConsumerGroup
	// - Redis Streams: ConsumerGroup
	ConsumerGroup() string

	// Unmarshal unmarshals the message body into the given value.
	Unmarshal(v interface{}) error

	// Marshal marshals the given value into the message body.
	Marshal(v interface{}) error
}

// ************************************************
// * Broker
// ************************************************

// Broker is the interface that all message brokers must implement.
type Broker interface {
	// Consume consumes a message from the given topics.
	// This sends messages to the returned channel.
	// Args:
	// - ctx: The context to use for the operation.
	// - cg: The consumer group to consume from.
	// - topics: The topics to consume from.
	NewConsumer(opts ConsumerOptions) (Consumer, error)

	// Publish publishes the given message to given topics.
	Publish(ctx context.Context, msg BrokerMessage, topics ...string) error

	// AckBrokerMessage acknowledges the given message.
	AckMessage(ctx context.Context, msg BrokerMessage) error

	// Close closes the message broker and all its consumers.
	Close() error

	// Connect connects the message broker.
	Connect(ctx context.Context) error

	// Options returns the broker options.
	Options() *BrokerOptions
}

// ************************************************
// * Consumer
// ************************************************

// Consumer is the interface that all message consumers must implement.
type Consumer interface {
	// Consume consumes a message from the given topics.
	// This sends messages to the returned channel.
	// Args:
	// - ctx: The context to use for the operation.
	// - cg: The consumer group to consume from.
	// - topics: The topics to consume from.
	Consume(ctx context.Context) (<-chan BrokerMessage, error)

	// Pause pauses the consumer.
	Pause()

	// Resume resumes the consumer.
	Resume()

	// Close closes the consumer.
	Close() error

	// Connect connects the consumer.
	Connect(ctx context.Context) error

	// Topics returns the topics that the consumer is consuming from.
	Topics() []string

	// ConsumerGroup returns the consumer group that the consumer is consuming from.
	ConsumerGroup() string
}
