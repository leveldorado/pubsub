package pubsub

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
)

func TestQueue(t *testing.T) {
	q := Queue{}
	subscriberOne, subscriberTwo := uuid.New().String(), uuid.New().String()
	topic := uuid.New().String()
	_, err := q.Poll(topic, subscriberOne)
	require.IsType(t, ErrSubscriptionNotFound{}, err)

	q.Subscribe(topic, subscriberOne)
	msg := uuid.New().String()
	q.Publish(topic, msg)
	_, err = q.Poll(topic, subscriberTwo)
	require.IsType(t, ErrSubscriptionNotFound{}, err)

	q.Subscribe(topic, subscriberTwo)
	value, err := q.Poll(topic, subscriberTwo)
	require.NoError(t, err)
	require.Nil(t, value)

	q.Subscribe(topic, subscriberOne)
	value, err = q.Poll(topic, subscriberOne)
	require.NoError(t, err)
	require.Equal(t, msg, value)

	msg = uuid.New().String()
	msgTwo := uuid.New().String()
	q.Publish(topic, msg)
	q.Publish(topic, msgTwo)

	for _, sub := range []string{subscriberOne, subscriberTwo} {
		value, err = q.Poll(topic, sub)
		require.NoError(t, err)
		require.Equal(t, msg, value)

		value, err = q.Poll(topic, sub)
		require.NoError(t, err)
		require.Equal(t, msgTwo, value)

		value, err = q.Poll(topic, sub)
		require.NoError(t, err)
		require.Nil(t, value)
	}
}

func BenchmarkQueue(b *testing.B) {
	topic := uuid.New().String()
	subscribers := prepareStringsList(1000)
	q := Queue{}
	for _, sub := range subscribers {
		q.Subscribe(topic, sub)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			q.Publish(topic, uuid.New().String())
			q.Poll(topic, subscribers[rand.Intn(len(subscribers))])
		}
	})
}

func prepareStringsList(n int) []string {
	var list []string
	for i := 0; i < n; i++ {
		list = append(list, uuid.New().String())
	}
	return list
}
