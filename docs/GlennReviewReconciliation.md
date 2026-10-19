# Glenn review reconciliation for PR 229

Updated 19 October 2026. Baseline: `239916db4c52f87f0eacd26b7854e10b43fb5773`.
This tracks source work against [Glenn’s review](https://github.com/reactiveui/Primitives/pull/229).
Baseline CI results do not certify the breaking framework revision below.

## Framework boundary

Applications compose the components and own domain policy, credentials and deployment.
The framework must provide neutral contracts, bounded work and atomic durable boundaries.
Optional adapters do not all belong in the first release.
See [remaining work](RemainingTasks.md) for acceptance criteria and original specification sections.
See [items 1 to 15](GlennReviewTracking.md) for the earlier source and regression evidence.

## Review threads

Rows use the review thread order, not the original 1–33 work-list numbers.
GitHub resolution records reviewer state. It does not prove that the current source meets the comment.
Source completion does not imply reviewer approval or a passing current-head CI gate.

| Thread | Topic | Source state | GitHub |
| --- | --- | --- | --- |
| [1](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826255) | Solution filter packages | Implemented in baseline — retain regression evidence. | Open |
| [2](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826264) | Unsupported parallelism setting | Implemented — ignored keys removed from both configurations; coverage filters unchanged. | Open |
| [3](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826267) | Coverage configuration duplication | Open — see remaining acceptance in items 16–33. | Open |
| [4](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826273) | Symbols and build comments | Open — see remaining acceptance in items 16–33. | Open |
| [5](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826277) | README package family selection | Implemented — one package-family prefix replaces the sixteen-name condition. | Open |
| [6](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826283) | Stable net11 package policy | Implemented in baseline — retain regression evidence. | Resolved |
| [7](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826290) | Unrelated span operator change | Open — see remaining acceptance in items 16–33. | Open |
| [8](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826295) | Release gate scope | Open — see remaining acceptance in items 16–33. | Open |
| [9](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826300) | CI tool analyzers | Open — see remaining acceptance in items 16–33. | Open |
| [10](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826303) | Obsolete SQLite dependency backlog | Implemented in baseline — retain regression evidence. | Resolved |
| [11](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826310) | Plain English mergeable names | Partial — mergeable API renamed; remaining limits are item 31. | Open |
| [12](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826316) | Public resilience surface | Partial — concrete implementations internal; audit remaining extension surface. | Open |
| [13](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826321) | Test randomness exposure | Implemented locally — test-only randomness internal. | Open |
| [14](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826324) | Effective element bounds | Implemented in baseline — retain regression evidence. | Open |
| [15](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826330) | Equal stamp merge determinism | Implemented in baseline — retain regression evidence. | Resolved |
| [16](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826335) | Causal removed tag pruning | Implemented in baseline — retain regression evidence. | Resolved |
| [17](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826344) | Merge validation complexity | Open — see remaining acceptance in items 16–33. | Open |
| [18](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826348) | Payload copies and equality | Open — see remaining acceptance in items 16–33. | Open |
| [19](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826353) | Unused security options | Open — see remaining acceptance in items 16–33. | Open |
| [20](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826357) | Constructible option defaults | Implemented locally — nested defaults initialized. | Open |
| [21](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826363) | Fake union markers | Implemented — 5.1.4 corrects metadata reports; two true alternatives now use closed case composition. All eight Core targets build. | Open |
| [22](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826369) | Sibling only Core plumbing | Open — see remaining acceptance in items 16–33. | Open |
| [23](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826378) | Ordered operation identifiers | Implemented locally — version 7 on supported targets with opt-out. | Open |
| [24](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826383) | Custom observer dispatcher | Open — see remaining acceptance in items 16–33. | Open |
| [25](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826392) | Duplicated scheduler contract | Open — see remaining acceptance in items 16–33. | Open |
| [26](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826399) | Unused custom admission queues | Open — see remaining acceptance in items 16–33. | Open |
| [27](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826405) | Production test utilities | Open — see remaining acceptance in items 16–33. | Open |
| [28](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826409) | Local commit serialization | Open — see remaining acceptance in items 16–33. | Open |
| [29](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826411) | Repeated payload validation | Open — see remaining acceptance in items 16–33. | Open |
| [30](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826416) | Per observer task hops | Open — see remaining acceptance in items 16–33. | Open |
| [31](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826419) | Custom timer delay | Open — see remaining acceptance in items 16–33. | Open |
| [32](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826423) | Host trace sampling | Implemented — host listeners own sampling; the library no longer drops traces. | Open |
| [33](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826433) | Interface downcasts | Open — see remaining acceptance in items 16–33. | Open |
| [34](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826439) | Broadcast operation waiters | Open — see remaining acceptance in items 16–33. | Open |
| [35](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826443) | Duplicate API baseline entry | Open — see remaining acceptance in items 16–33. | Open |
| [36](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826449) | Blocking DI resolution | Open — see remaining acceptance in items 16–33. | Open |
| [37](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826453) | Startup exception logging | Implemented — redacted default with explicit opt-in to the original exception; logger failures remain isolated. | Open |
| [38](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826459) | Terminal health failure | Implemented locally — fault latch and composed subscription; 11 TUnit tests pass. | Open |
| [39](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826465) | Forced platform storage dependencies | Open — see remaining acceptance in items 16–33. | Open |
| [40](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826470) | Neutral connectivity input | Open — see remaining acceptance in items 16–33. | Open |
| [41](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826477) | Raw SQLite provider | Implemented in baseline — retain regression evidence. | Resolved |
| [42](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826482) | Whole database encryption | Open — see remaining acceptance in items 16–33. | Open |
| [43](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826486) | SQLite synchronous policy | Open — see remaining acceptance in items 16–33. | Resolved |
| [44](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826492) | Point read authentication | Implemented in baseline — retain regression evidence. | Resolved |
| [45](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826496) | Derived key reuse | Implemented in baseline — retain regression evidence. | Resolved |
| [46](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826501) | Rollback threat boundary | Open — see remaining acceptance in items 16–33. | Open |
| [47](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826504) | Neutral server SQLite dependency | Open in source despite GitHub resolution — dependency boundary still requires revision. | Resolved |
| [48](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826510) | Database wide capacity scans | Implemented in baseline — retain regression evidence. | Resolved |
| [49](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826515) | Journal query amplification | Implemented in baseline — retain regression evidence. | Resolved |
| [50](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826517) | Journal indexes | Implemented in baseline — retain regression evidence. | Open |
| [51](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826523) | Repeated schema inspection | Implemented in baseline — retain regression evidence. | Resolved |
| [52](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826528) | Connection lifetime | Implemented in baseline — retain regression evidence. | Resolved |
| [53](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826534) | Idle subscriber writes | Implemented in baseline — retain regression evidence. | Resolved |
| [54](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826539) | Subscriber wakeups | Implemented in baseline — retain regression evidence. | Resolved |
| [55](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826541) | Bounded server scheduling | Implemented in baseline — retain regression evidence. | Resolved |
| [56](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826545) | Finite deduplication retention | Implemented in baseline — retain regression evidence. | Resolved |
| [57](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826549) | Unused replay registry | Open — see remaining acceptance in items 16–33. | Open |
| [58](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826556) | HTTP shaped SignalR server | Open — see remaining acceptance in items 16–33. | Open |
| [59](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826559) | SignalR host isolation | Implemented locally — isolated protocol and host options; 71 tests pass on net8/net10. | Open |
| [60](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826564) | WebSocket host configuration | Partial — socket hook verified by 28 tests on net8/net10; server/liveness remain open. | Open |
| [61](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826569) | Independent receive lanes | Implemented in baseline — retain regression evidence. | Resolved |
| [62](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826577) | Terminal session fault propagation | Implemented in baseline — retain regression evidence. | Open |
| [63](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826582) | Wire overhead | Optional adapter/design decision — not required by framework scope alone. | Open |
| [64](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826588) | Manual JSON validation | Open — see remaining acceptance in items 16–33. | Open |
| [65](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826591) | Replay topology and lookup | Open — see remaining acceptance in items 16–33. | Open |
| [66](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826593) | Temporary signing proof | Implemented — unsigned signing input carries no fake proof; real proof headers and strict admission tests pass. | Open |
| [67](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826596) | Empty HTTP polling | Implemented — cancellable elapsed-time pacing for empty replies; no added delay for data or normal long polls. | Open |
| [68](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826600) | Incremental file journal | Implemented in baseline — retain regression evidence. | Resolved |
| [69](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826604) | Corrupt journal tail policy | Explained — complete checksum corruption fails closed; only incomplete final frames are discarded. | Open |
| [70](https://github.com/reactiveui/Primitives/pull/229#discussion_r4151826610) | LiteDB document bounds | Open — see remaining acceptance in items 16–33. | Open |
| [71](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460405) | Version 7 identifiers | Implemented locally — version 7 defaults and compatibility tests. | Open |
| [72](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460411) | Prepared SQLite statement reuse | Implemented in baseline — retain regression evidence. | Resolved |
| [73](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460414) | SQLite key and rekey | Open — see remaining acceptance in items 16–33. | Open |
| [74](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460421) | Provider exception in Core | Open — see remaining acceptance in items 16–33. | Open |
| [75](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460427) | LiteDB AOT claims | Open — see remaining acceptance in items 16–33. | Open |
| [76](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460432) | Normal parent path aliases | Partial — physical path alias resolution exists; device proof remains. | Open |
| [77](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460438) | Mounted network storage detection | Partial — longest mounted path policy exists; target proof remains. | Open |
| [78](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460443) | Apple durable flush | Implemented — enable and verify SQLite fullfsync and checkpoint_fullfsync on Apple. Hardware proof remains. | Open |
| [79](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460450) | Native Mobile package assets | Implemented in baseline — retain regression evidence. | Open |
| [80](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460459) | Installation and key recovery | Implemented in baseline — retain regression evidence. | Open |
| [81](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460488) | Mobile drain budget | Open — see remaining acceptance in items 16–33. | Open |
| [82](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460493) | File replacement durability | Implemented — Unix parent-directory sync and bounded Windows sharing retries. Unix execution proof remains. | Open |
| [83](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460499) | Monotonic retry age | Open — see remaining acceptance in items 16–33. | Open |
| [84](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460504) | Recoverable replay clock skew | Open — see remaining acceptance in items 16–33. | Open |
| [85](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460509) | Browser SQLite rejection | Implemented — reject before creating the dedicated worker. Browser execution proof remains. | Open |
| [86](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460515) | Browser persistence and quota | Open — see remaining acceptance in items 16–33. | Open |
| [87](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460520) | Lifecycle JavaScript loop | Implemented in baseline — retain regression evidence. | Open |
| [88](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460526) | Hidden versus frozen browser policy | Open — see remaining acceptance in items 16–33. | Open |
| [89](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460534) | Browser disposal join | Explained — disposal cancels operations and drains accepted work before joining cleanup. | Open |
| [90](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460540) | Reason code consistency | Open — see remaining acceptance in items 16–33. | Open |
| [91](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460545) | Mergeable kind names | Partial — plain English kinds implemented; final multi-target validation pending. | Open |
| [92](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460548) | Mergeable member names | Partial — element/tag/value mappings implemented; final validation pending. | Open |
| [93](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460552) | Maximum limit names | Open — see remaining acceptance in items 16–33. | Open |
| [94](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460559) | Publish and Upload terminology | Open — see remaining acceptance in items 16–33. | Open |
| [95](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460563) | Status record terminology | Open — see remaining acceptance in items 16–33. | Open |
| [96](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460573) | Payload migration names | Open — see remaining acceptance in items 16–33. | Open |
| [97](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460579) | Unreadable payload terminology | Open — see remaining acceptance in items 16–33. | Open |
| [98](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460590) | Snapshot builder names | Open — see remaining acceptance in items 16–33. | Open |
| [99](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460599) | Server state updater names | Open — see remaining acceptance in items 16–33. | Open |
| [100](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460606) | Sync server naming | Open — see remaining acceptance in items 16–33. | Open |
| [101](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460613) | External ownership names | Open — see remaining acceptance in items 16–33. | Open |
| [102](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460621) | Send attempt names | Open — see remaining acceptance in items 16–33. | Open |
| [103](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460623) | Unknown outcome terminology | Open — see remaining acceptance in items 16–33. | Open |
| [104](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460628) | State reducer terminology | Open — see remaining acceptance in items 16–33. | Open |
| [105](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460635) | Queued SQLite write limits | Open — see remaining acceptance in items 16–33. | Open |
| [106](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460641) | Request authorization names | Open — see remaining acceptance in items 16–33. | Open |
| [107](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460649) | DI abstraction dependency | Implemented — adapter depends on DI abstractions; 87 tests pass. | Resolved |
| [108](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460654) | Generic Host scope | Open — document Generic Host integration separately from native lifecycle. | Open |
| [109](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460660) | Language neutral wire contract | Open — see remaining acceptance in items 16–33. | Open |
| [110](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460666) | Mergeable interoperability scope | Open — document server-mediated merge scope; do not claim rich-document interoperability. | Open |
| [111](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460671) | Optional MQTT adapter | Optional adapter/design decision — not required by framework scope alone. | Open |
| [112](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460681) | Optional streaming transports | Optional adapter/design decision — not required by framework scope alone. | Open |
| [113](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460689) | Standard request authentication | Open — see remaining acceptance in items 16–33. | Open |
| [114](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460698) | Direct SignalR protocol | Open — see remaining acceptance in items 16–33. | Open |
| [115](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460706) | Optional CloudEvents | Optional adapter/design decision — not required by framework scope alone. | Open |
| [116](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460710) | Negotiated compression | Optional adapter/design decision — not required by framework scope alone. | Open |
| [117](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460714) | Strict JSON duplicate handling | Open — see remaining acceptance in items 16–33. | Open |
| [118](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460717) | Internal resilience implementation | Implemented locally — internal resilience with public extension point. | Resolved |
| [119](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460722) | Host telemetry policy | Partial — host sampling is fixed; metric units and meter-factory work remain open. | Open |
| [120](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460726) | Bound configuration validation | Open — see remaining acceptance in items 16–33. | Open |
| [121](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460737) | Connectivity and battery policy | Open — see remaining acceptance in items 16–33. | Open |
| [122](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460747) | Local store conformance | Implemented in baseline — retain regression evidence. | Open |
| [123](https://github.com/reactiveui/Primitives/pull/229#discussion_r4152460752) | Provider neutral platform composition | Open — see remaining acceptance in items 16–33. | Open |

## Current validation boundary

Core tests pass 592/592 on net10 and net11. All eight Core targets build with the new case models.
HTTP tests pass 986/986 on net10 and net11, including unsigned signing and eleven idle-poll regressions.
DI tests pass 87/87 with full package line and branch coverage. Activity tests pass 15/15 under coverage.
The full feature gate and current-head CI must pass before release readiness is claimed.
The storage follow-up passes 155 filesystem tests on both net10 and net11 on Windows.
Server tests pass 598/598 on net11. SQLite platform and connection checks pass 26 tests.
Windows coverage cannot verify Unix directory synchronization or Apple hardware flush behavior.
No analyzer severity, coverage filter or quality threshold is weakened.

Provider separation, the neutral atomic server persistence seam and the remaining naming groups are still open.
This revision does not close GitHub threads or claim release readiness.
