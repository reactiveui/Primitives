# Remaining Glenn review tasks

- **16. Platform durability:** Define and test Apple full-flush behavior, directory sync after file replacement, network-drive restrictions and bounded Windows replacement retries.
- **17. LiteDB capacity and AOT:** Keep default limits within the document size limit. Verify trimming and native ahead-of-time compilation claims in a consuming app.
- **18. Clock resilience:** Test forward and backward clock changes, restart deadlines and client/server replay clock skew.
- **19. Browser storage limits:** Request persistent storage when supported. Report denial, quota exhaustion and eviction risks. Reject unsupported browser worker configurations.
- **20. Platform lifecycle:** Define bounded behavior for hidden or frozen pages, mobile suspension, background execution and Android Doze.
- **21. Transport configuration and liveness:** Complete WebSocket credentials, proxy, certificate and keep-alive options. Prevent empty HTTP responses from causing a busy loop.
- **22. SignalR isolation:** Scope serialization registration to the sync hub without changing unrelated application hubs.
- **23. Replay protocol:** Ensure temporary proof markers cannot reach the wire. Verify secret handling, bounded replay state and multi-node hosting behavior.
- **24. Payload costs:** Remove unnecessary copying, parsing and hashing without changing ownership, equality or validation.
- **25. Runtime scheduling:** Reuse existing scheduling and waiter helpers where their behavior matches. Reduce per-subscriber task creation and arbitrary wait limits.
- **26. Public contracts:** Fix ineffective options and inconsistent defaults. Verify public extensions, supported interfaces and target-framework API baselines.
- **27. DI and health:** Remove synchronous initialization blocking. Improve startup diagnostics and ensure terminal faults cannot leave health status healthy.
- **28. Configuration and observability:** Decide configuration binding and startup validation. Keep telemetry sampling under host control and avoid logging secrets.
- **29. Release maintenance:** Verify symbol publication, supported test parallelism, package metadata and release gates. Remove or justify unrelated changes.
- **30. Release-facing claims:** Update package and suite counts in the PR and release notes. Distinguish handwritten, generated, aggregate and new-code coverage.
- **31. API decisions:** Agree plain-English names, provider-neutral boundaries, internal test seams, operation terminology, identifier defaults and compatibility policy.
- **32. Architecture and scope decisions:** Agree first-release package boundaries, store choice for platform adapters, journal separation, encryption architecture and wire format. Decide whether optional MQTT, IoT, gRPC, SSE or compression belongs in a later release.
- **33. Final review reconciliation:** Give every remaining review thread a fix, verified explanation or agreed deferral. Run the relevant release, platform, AOT and CI checks before resolving it.
