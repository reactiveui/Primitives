# OccasionallyConnected implementation

The normative feature specification is [ReactiveUI.Primitives.OccasionallyConnected.md](ReactiveUI.Primitives.OccasionallyConnected.md).
Implementation PRs target `OccasionallyConnected`. The feature is incomplete until all v1 gates below pass.

## Delivery stages

| Stage | Scope | Required behavioral evidence |
| --- | --- | --- |
| 1 | Core identities, options, models and contracts | Unicode and size boundaries, immutable options, invalid configuration and capability combinations |
| 2 | Allowlisted JSON serialization and versioning | Canonical-byte hashes, unknown contracts, upcast chains, corrupt payloads, AOT registration |
| 3 | Builder and context lifecycle | Structural validation, identity compatibility, concurrent start/stop and restart |
| 4 | Sequenced local publishing | Atomic optimistic commit, cancellation boundaries, serialized notifications and observer isolation |
| 5 | Bounded queues and observer input | Count and byte limits, every overflow policy, durable records never dropped |
| 6 | Retry, batching and loopback transport | Lost ACKs, stable operation IDs, virtual time, circuit transitions and fairness |
| 7 | Remote receive and inbox | Deduplication, durable cursors, post-commit ACKs and snapshot recovery |
| 8 | Server authorization and idempotency | Authenticated identity, replay authorization, atomic effects and original duplicate results |
| 9 | SQLite persistence | Transaction conformance, child-process crash points, leases, migration and compaction |
| 10 | HTTP protocol | Version fixtures, bounded parsing, nonce replay protection, partial and ambiguous results |
| 11 | Exactly-once effect | Capability negotiation, retention expiry and explicit downgrade |
| 12 | DI and release verification | Options validation, samples, packaging, API, trim/AOT, security, soak and performance |

Each stage may use multiple small PRs. Tests precede behavior changes and use TUnit assertions exclusively.
Every stage requires zero build/analyzer warnings and 100% line and branch coverage for its new executable code;
the user-required coverage gate supersedes the lower percentages in specification section 17.4.
No suppression or coverage exclusion is added to meet these gates. Coverage totals alone do not establish the
durability, ordering or security invariants: the scenario and conformance suites must also pass.

## Integration decisions

- Public API baselines use the repository's current `PublicAPI/<tfm>/PublicAPI.txt` format and PublicApiSharp analyzer.
  The specification's shipped/unshipped filenames describe an older mechanism; API changes remain reviewable per TFM.
- Core targets the centrally defined `LibraryTargetFrameworks`; tests use `TestTargetFrameworks`.
- The six v1 packages are Core, the lean runtime, Server, Storage.Sqlite, Transport.Http and DependencyInjection.
  The specification explicitly schedules the separate Hosting and Reactive variants and other adapters after v1.
- New capabilities remain unadvertised until their conformance suite passes.
- Feature tests use `src/occasionally-connected.testconfig.json`, with no source, function or attribute exclusions.
  The existing preview-package warning suppression does not apply to the new package family.
- `StartPosition.FromSequence` denotes a non-negative server sequence interpreted by the adapter, never a local
  client sequence. Cursor values remain opaque and are limited to 4096 UTF-8 bytes without normalization.

## Contract decisions to establish before dependent implementation

The design repeats `ClientIdentity` with different constructor shapes, leaves several policy interfaces implicit,
and requires recovery transactions beyond the abbreviated adapter snippets. Record the final semantics alongside
the stage introducing each contract, with executable tests. Do not silently weaken the normative guarantees to
fit a snippet.

## Verification record

### Stage 1a: stream identities and start positions

- Executable RED: `StreamId` stub failed 16 of 28 tests; `StartPosition` stub failed 6 of 12 tests.
- GREEN: 40 tests passed on each of net8.0, net9.0, net10.0 and net11.0 (160 executions).
- Release coverage, independently inspected through Mtpunittestmcp: 66/66 lines and 58/58 branches on each modern TFM.
- Release builds and API baseline checks passed all eight library TFMs with zero warnings; NuGet packing passed.
- No new suppression or source/function/attribute coverage exclusion was added.
- Logs and reports are generated under `artifacts/occasionally-connected`; CI retains fresh reports for each OS/TFM.

The installed .NET 11 SDK's native `dotnet test` handshake returned exit 5 for this TUnit application. Building the
test project and executing its DLL directly runs the same Microsoft.Testing.Platform/TUnit application successfully.
The coverage workflow uses that invocation on Windows, Linux and macOS, with an explicit 100% line/branch gate.
PR [#192](https://github.com/reactiveui/Primitives/pull/192) passed all twelve feature coverage jobs on Windows,
Linux and macOS across the four modern frameworks. Full-solution CI builds remain a separate check.

### Stage 1b: publishing, subscription and observer input options

- Added operation/subscription identities and immutable options with structural validation.
- Durable publishing rejects dropping policies; exactly-once publishing requires durability; synchronous observer
  input rejects blocking backpressure. Custom policies require explicit registration support.
- Priority validation accepts configured inclusive bounds, with a default of -10 through 10.
- Agent tests preceded implementation; root review added independent custom-policy, durable-guarantee and
  per-operation override cases. Disabling validation caused 29 tests to fail.
- GREEN: 90 tests passed on each modern framework (360 executions).
- Release coverage independently inspected through Mtpunittestmcp: 152/152 lines and 104/104 branches per framework.

Only the identities and local option validation are verified. Adapter capability negotiation, remaining contracts
and every runtime/durability stage are still incomplete. Passing option validation alone does not establish a
delivery guarantee or establish that a custom policy preserves durable work; the runtime must enforce both.
