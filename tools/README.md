# tools

Maintenance scripts for the ReactiveUI.Primitives repository.

## OccasionallyConnected CI gates

The .NET CLI in `OccasionallyConnected.Ci` runs the coverage, package, supply-chain,
mutation, and NativeAOT gates. Run it from the repository root with a .NET 10 SDK:

```sh
dotnet run --project tools/OccasionallyConnected.Ci/OccasionallyConnected.Ci.csproj -- coverage --framework net10.0 --run-id 123 --run-attempt 1 --runner-os Linux
dotnet run --project tools/OccasionallyConnected.Ci/OccasionallyConnected.Ci.csproj -- packages --version 0.1.0-ocpkg.123.1 --skip-aot
dotnet run --project tools/OccasionallyConnected.Ci/OccasionallyConnected.Ci.csproj -- supply-chain --version 0.1.0-ocscan.123
dotnet run --project tools/OccasionallyConnected.Ci/OccasionallyConnected.Ci.csproj -- mutation --campaign Durability
dotnet run --project tools/OccasionallyConnected.Ci/OccasionallyConnected.Ci.csproj -- aot --version 0.1.0-ocaot.123.1
```

Coverage checks the complete OccasionallyConnected test suite list on net8.0,
net9.0, net10.0, or net11.0. Each adapter runs only on its supported frameworks.
This matrix runs on every pull request and push to main.
The Build workflow tests the other suites. It still builds every project.
This split avoids repeating the feature matrix within one test-run deadline.
Sonar builds every target. It runs the other suites on every supported target.
It runs all feature suites on .NET 10 and imports their reports for source coverage.
The required feature matrix still verifies the other frameworks.
Sonar passes `--no-build` to reuse its successful Release build.
Use this option only after building the requested test framework.
The gate still requires every suite and a fresh coverage report from each suite.
The runtime and SQLite suites share CPU-sized test budgets within each host.
This bounds competing database fixtures. Each test still runs its own concurrent
operations and keeps its original assertions and deadlines.
To check existing Cobertura reports without running tests, use `coverage` with
repeated `--report-path <file>` and `--package-name <name>` options instead.
The package command also accepts `--sample-target-frameworks <tfm,tfm>`,
`--skip-determinism`, and `--skip-sample`. Supply-chain accepts repeated
`--project-name <name>` options. Packages, supply-chain, and AOT accept
`--artifacts-path <path>`. The AOT gate requires a Windows x64 build host with
the native toolchain. Gate logs and reports remain under `artifacts/`.

## generate-publicapi

Regenerates the **PublicAPI baseline files** consumed by
`Microsoft.CodeAnalysis.PublicApiAnalyzers` (`RS0016`, `RS0017`, `RS0037`).

Each shipped library tracks its public surface per target framework in:

```
src/<Project>/PublicAPI/<tfm>/PublicAPI.Shipped.txt
src/<Project>/PublicAPI/<tfm>/PublicAPI.Unshipped.txt
```

When you add, remove, or change public API, those files must be updated or the build
fails with `RS0016` (symbol not in baseline) / `RS0017` (baseline entry not found) /
`RS0037` (missing `#nullable enable`). These scripts do that for you.

Tests, benchmarks, and source-generator projects are skipped structurally, so they
are never touched.

### Usage

Linux / macOS:

```bash
tools/generate-publicapi.sh                 # all tracked libraries, all buildable TFMs
tools/generate-publicapi.sh Async           # only projects whose path contains 'Async'
tools/generate-publicapi.sh ReactiveUI.Primitives.Core
```

Windows (PowerShell):

```powershell
./tools/generate-publicapi.ps1                  # all tracked libraries
./tools/generate-publicapi.ps1 -Filter Async    # path filter
```

The optional argument is a case-sensitive substring matched against each project's
path, so you can scope a run to a single library while iterating.

### What it does per TFM

1. Ensures `PublicAPI/<tfm>/PublicAPI.Shipped.txt` exists and preserves its current
   content as the analyzer input.
2. Resets only `PublicAPI.Unshipped.txt` to `#nullable enable`.
3. Runs `dotnet format analyzers <proj> -f <tfm> --diagnostics RS0016 RS0017 RS0037
   --severity info`, which adds missing API entries to `PublicAPI.Unshipped.txt` and
   lets the analyzer repair stale shipped entries.
4. Folds post-format **Shipped** plus generated **Unshipped** into
   `PublicAPI.Shipped.txt` (ordinally sorted, deduped), then resets
   `PublicAPI.Unshipped.txt` back to the bare header. The scripts refuse to replace
   a previously non-empty Shipped baseline with an empty baseline.

### Platform notes

* Run the script on the OS that can build the frameworks you need:
  * **Windows** builds the .NET Framework and Windows-desktop (WPF / WinForms / WinUI)
    TFMs natively, plus Android and — with the matching workloads — the Apple TFMs.
    Use `generate-publicapi.ps1`. This repo's Windows/Apple legs are generated on the
    dockur Windows guest (`~/dockur-windows`); the repo is shared in-guest as
    `\\host.lan\Data\rxui\Primitives`.
  * **Linux** builds `net8.0+`, Android, and the Windows-desktop TFMs cross-platform
    (the script sets `EnableWindowsTargeting=true`). It cannot produce Apple
    (`-ios` / `-maccatalyst` / `-tvos` / `-macos`) baselines.
  * **macOS** additionally builds the Apple TFMs.
* A target framework whose workload or SDK is missing is **skipped with a warning**
  (its seed files are left in place); the rest of the run continues. The script's
  exit code is non-zero if any TFM failed, so CI can detect an incomplete run.
* **Android caveat:** the generators build through `dotnet format` (in-memory), which
  does not run the Android Resource designer. The scripts add the generated
  `<RootNamespace>.Resource` entries to `net*-android` Shipped baselines explicitly
  because the real `-c Release` build emits that type:
  ```
  <RootNamespace>.Resource
  <RootNamespace>.Resource.Resource() -> void
  ```
* The scripts set `MinVerVersionOverride` (default `255.255.255-dev`) so versioning
  does not depend on git history; override it by exporting/setting the variable first.

### When to run

* After changing any public (or `protected` on a public type) API.
* After adding a new target framework to a tracked library.
* After bumping an analyzer package that changes how the public surface is rendered.

Review the resulting `PublicAPI.Shipped.txt` diff before committing — it is the
human-auditable record of your public API change. `PublicAPI.Unshipped.txt` should
return to just `#nullable enable`.
