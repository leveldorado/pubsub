This implementation of pub-sub has list of messages per topic-subscription.
So every messages duplicates n (subscription) times.

In terms of big-O notation:

the message publish algorithm complexity - O(n) where n is number of subscriptions
the message poll algorithm complexity - O(1)
the memory (space) complexity - O(n*m) where number of messages and m number of subscription 

How it can be improved:

There can be just one list per topic and subscriptions keep track of last read element.
In that case publish complexity comes to O(1) and memory complexity comes to O(n+m).
But the algorithm will take more time on implementation (tricky part is to clean read messages by all subscriptions).
