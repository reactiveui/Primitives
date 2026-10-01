# ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets

This package provides a bidirectional WebSocket implementation of
`IRemoteTransportAdapter` for occasionally connected synchronization.

The adapter performs one capability handshake when `ConnectAsync` is called.
It uses bounded JSON text frames, correlates request and response messages, and
delivers subscription events through a bounded stream. It does not reconnect,
retry, or apply backoff. Those policies belong to the synchronization engine.

The adapter advertises batch push, cursor resume, receive acknowledgements,
server idempotency, atomic apply-and-acknowledge, and streaming receive.
Snapshot recovery is not advertised because this transport does not implement
it.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

## Subscription limits and recovery

Each subscription holds at most 64 batches. `MaximumBufferedSubscriptionBytes`
also limits the queued wire bytes. Its default is 4 MiB. The receiver never waits
for space in an event buffer. A full buffer faults that subscription with
`subscription-overflow`. Other subscriptions, push responses and acknowledgements
continue on the same socket.

The adapter releases undelivered batches without acknowledging them. The engine
retries from the last cursor it saved after applying a batch. Your peer must retain
unacknowledged events and honor cursor resume. It must also deduplicate repeated
operation IDs. Overflow does not advance a cursor or change an operation ID.

## Session failures

The first receiver failure closes request admission. Pending and future requests
receive the same failure without waiting for caller cancellation. Dispose cancels
receiving and sending, waits for admitted requests, and releases socket resources.
A failed handshake also disposes its session.

`WebSocketRemoteTransportException` implements `IRemoteTransportFailure`.
Closed sockets and transport errors have an ambiguous outcome. A subscription
overflow is transient. Oversized messages are payload-size failures.
Malformed frames and unknown peer error codes are validation failures.
The engine owns retry and reconnection policy.
