package pubsub

import (
	"container/list"
	"sync"
)

/*
ErrSubscriptionNotFound returns on poll in case subscribe has not been done before
*/
type ErrSubscriptionNotFound struct{}

func (ErrSubscriptionNotFound) Error() string { return "Subscription not found" }

/*
Queue contains list for every subscription. so messages are duplicated N subscriptions times per topic
*/
type Queue struct {
	sync.RWMutex
	topics map[string]topic
}

type topic struct {
	sync.RWMutex
	subscriptions map[string]subscription
}

type subscription struct {
	sync.Mutex
	list *list.List
}

func (q *Queue) Publish(topicName string, body interface{}) {
	q.RLock()
	t := q.topics[topicName]
	t.RLock()
	q.RUnlock()
	if len(t.subscriptions) == 0 {
		t.RUnlock()
		return
	}
	for _, sub := range t.subscriptions {
		sub.Lock()
		sub.list.PushBack(body)
		sub.Unlock()
	}
	t.RUnlock()
}

func (q *Queue) Subscribe(topicName, subscriberName string) {
	q.Lock()
	t, ok := q.topics[topicName]
	if !ok {
		if q.topics == nil {
			q.topics = map[string]topic{}
		}
		q.topics[topicName] = topic{subscriptions: map[string]subscription{subscriberName: {list: list.New()}}}
		q.Unlock()
		return
	}
	t.Lock()
	q.Unlock()
	_, ok = t.subscriptions[subscriberName]
	if ok {
		t.Unlock()
		return
	}
	t.subscriptions[subscriberName] = subscription{list: list.New()}
	t.Unlock()
}

func (q *Queue) Unsubscribe(topicName, subscriberName string) {
	q.Lock()
	delete(q.topics[topicName].subscriptions, subscriberName)
	q.Unlock()
}

func (q *Queue) Poll(topicName, subscriberName string) (interface{}, error) {
	q.RLock()
	sub, ok := q.topics[topicName].subscriptions[subscriberName]
	if !ok {
		q.RUnlock()
		return nil, ErrSubscriptionNotFound{}
	}
	sub.Lock()
	q.RUnlock()
	el := sub.list.Front()
	if el == nil {
		sub.Unlock()
		return nil, nil
	}
	body := sub.list.Remove(el)
	sub.Unlock()
	return body, nil
}
