package gomb

import (
	"context"
	"time"
)

// ! all the broker implementations use this message struct

// Message is the struct that all brokers handle.
// It implements the Message interface.
type Message struct {
	// The topic that the message was published to.
	topic string

	// the key of the message
	// does not apply to all brokers, namely redis streams
	key string

	// The message body.
	body []byte

	// The message headers.
	headers map[string]string

	// The message ID.
	id string

	// The message timestamp.
	timestamp time.Time

	// The consumer group that the message was consumed from.
	consumerGroup string

	// the group that the message was consumed from.
	// only used by redis streams
	group string

	// The message broker, for Ack() and Republish().
	broker Broker
}

// ************************************************
// * Init
// ************************************************

// NewMessage creates a new message.
func NewMessage(topic string, key string, body []byte, headers map[string]string, id string, timestamp time.Time, broker Broker) *Message {
	return &Message{
		key:       key,
		topic:     topic,
		body:      body,
		headers:   headers,
		id:        id,
		timestamp: timestamp,
		broker:    broker,
	}
}

// SetBroker sets the message broker.
func (m *Message) SetBroker(broker Broker) {
	m.broker = broker
}

// SetBody sets the message body.
func (m *Message) SetBody(body []byte) {
	m.body = body
}

// SetHeaders sets the message headers.
func (m *Message) SetHeaders(headers map[string]string) {
	m.headers = headers
}

// SetID sets the message ID.
func (m *Message) SetID(id string) {
	m.id = id
}

// SetTimestamp sets the message timestamp.
func (m *Message) SetTimestamp(timestamp time.Time) {
	m.timestamp = timestamp
}

// SetTopic sets the message topic.
func (m *Message) SetTopic(topic string) {
	m.topic = topic
}

// SetConsumerGroup sets the message consumer group.
func (m *Message) SetConsumerGroup(consumerGroup string) {
	m.consumerGroup = consumerGroup
}

// SetGroup sets the message group.
func (m *Message) SetGroup(group string) {
	m.group = group
}

// SetKey sets the message key.
func (m *Message) SetKey(key string) {
	m.key = key
}

// ************************************************
// * Data methods
// ************************************************

// Body returns the message body.
func (m *Message) Body() []byte {
	return m.body
}

// Headers returns the message headers.
func (m *Message) Headers() map[string]string {
	return m.headers
}

// ID returns the message ID.
func (m *Message) ID() string {
	return m.id
}

// Timestamp returns the message timestamp.
func (m *Message) Timestamp() time.Time {
	return m.timestamp
}

// Topic returns the message topic.
func (m *Message) Topic() string {
	return m.topic
}

// ConsumerGroup returns the message consumer group.
func (m *Message) ConsumerGroup() string {
	return m.consumerGroup
}

// Group returns the message group.
func (m *Message) Group() string {
	return m.group
}

// Key returns the message key.
func (m *Message) Key() string {
	return m.key
}

// ************************************************
// * Message methods
// ************************************************

// Unmarshal unmarshals the message body into the given value, using the
// broker's Unmarshal function.
func (m *Message) Unmarshal(v interface{}) error {
	return m.broker.Options().Unmarshal(m.body, v)
}

// Marshal marshals the given value into the message body, using the
// broker's Marshal function.
func (m *Message) Marshal(v interface{}) error {
	// o := m.broker.Options()
	body, err := m.broker.Options().Marshal(v)
	if err != nil {
		return err
	}
	m.body = body
	return nil
}

// Ack acknowledges the message, using the broker's Ack function.
func (m *Message) Ack(ctx context.Context) error {
	return m.broker.AckMessage(ctx, m)
}

// Republish republishes the message, using the broker's Republish function.
func (m *Message) Republish(ctx context.Context) error {
	// simply publish the message again
	// using the already set topic
	// this will create a new message, with a new ID
	return m.broker.Publish(ctx, m, m.topic)
}

// Publish this message to the given topics.
func (m *Message) Publish(ctx context.Context, topics ...string) error {
	return m.broker.Publish(ctx, m, topics...)
}
