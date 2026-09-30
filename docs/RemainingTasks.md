# ReactiveUI.Primitives.OccasionallyConnected remaining tasks

- Remove `Microsoft.Data.Sqlite.Core` from the SQLite store and server journal. Use `SQLitePCLRaw.core` with `SQLite3MC.PCLRaw.bundle` for database access.
- Add the optional `ReactiveUI.Primitives.OccasionallyConnected.Mqtt` transport with explicit MQTT QoS mapping and end-to-end idempotency.
- Add the optional `ReactiveUI.Primitives.OccasionallyConnected.IoT` composition with embedded storage defaults and MQTT integration.
