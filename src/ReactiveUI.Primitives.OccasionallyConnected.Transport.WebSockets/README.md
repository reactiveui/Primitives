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
