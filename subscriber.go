package pubsub

import "sync"

type Subscribers map[string]Subscriber

// Message contains the topic and data to be communicated to interested parties
type Message struct {
	Topic string
	Body  string
}

func NewMessage(topic, message string) Message {
	return Message{
		Topic: topic,
		Body:  message,
	}
}

// Subscriber is the object that will register and unregister itsself for messages for a topic
type Subscriber struct {

	// Name of the subscriber
	ID string

	active bool

	messages chan Message

	// Map of topics currently subscribed to
	topics map[string]bool

	mutex sync.RWMutex
}

func (s *Subscriber) AddTopic(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.topics[topic] = true
}

func (s *Subscriber) RemoveTopic(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.topics, topic)
}

func (s *Subscriber) Destruct() {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.active = false
	close(s.messages)
}

type Broker struct {
	subscribers Subscribers
	topics      map[string]Subscribers
	lock        sync.RWMutex
}

func (b *Broker) Subscribe(s *Subscriber, topic string) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.topics[topic] == nil {
		b.topics[topic] = Subscribers{}
	}
}
