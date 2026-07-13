# CLAUDE.md

This file is the single source of truth for AI/agent assistance in this repository. It consolidates build/test commands,
repository layout, test runner usage, and the main constraints needed to work safely in `ReactiveUI.Primitives`.

If there is any conflict between other agent instruction files and this file, follow **CLAUDE.md**.

---

## Repository Orientation

- **Repository root:** `.`
- **Primary working directory for build/test:** `./src`
- **Main solution:** `src/ReactiveUI.Primitives.slnx`
- **Benchmarks project:** `src/benchmarks/ReactiveUI.Primitives.Benchmarks/ReactiveUI.Primitives.Benchmarks.csproj`
- **Tests:** `src/tests/`

---

## Solution Format: SLNX

This repository uses **SLNX** (XML-based solution format) instead of legacy `.sln`.

- Main file: `src/ReactiveUI.Primitives.slnx`
- Use `dotnet build` / `dotnet test` against the `.slnx` file the same way as a `.sln`

---

## Build Environment Requirements

### Working Directory Rule

**CRITICAL:** Run `dotnet` build/test commands from `./src`, not the repository root, unless the command explicitly uses
`src/`-prefixed paths.

Running `dotnet test` from the repository root can trigger Microsoft Testing Platform / VSTest invocation issues on .NET
10+ SDKs.

### Restore And Build

```bash
cd src

dotnet restore "ReactiveUI.Primitives.slnx"
dotnet build "ReactiveUI.Primitives.slnx"
dotnet build "ReactiveUI.Primitives.slnx" -c Release
dotnet clean "ReactiveUI.Primitives.slnx"
```

### Full Solution Test Command

```bash
cd src
dotnet test "ReactiveUI.Primitives.slnx"
```

Equivalent explicit invocation:

```bash
dotnet test "ReactiveUI.Primitives.slnx"
```

with `workdir` set to:

```text
/home/glennw/source/rxui/Primitives/src
```

---

## Testing: Microsoft Testing Platform (MTP) + TUnit

This repository uses **Microsoft Testing Platform (MTP)** with **TUnit**. This differs from VSTest.

- Test support is enabled centrally in `src/Directory.Build.props`
- Test execution settings live in `src/testconfig.json`
- Command-line filtering uses **TUnit/MTP** syntax, not NUnit/xUnit/VSTest filter syntax

### Testing Best Practices

- Do **not** use repository-root `dotnet test`
- Prefer building before testing rather than relying on stale binaries
- Place TUnit/MTP-specific arguments **after** `--`

### Test Commands

```bash
cd src

# Run all tests
dotnet test "ReactiveUI.Primitives.slnx"

# Run a specific project
dotnet test "tests/ReactiveUI.Primitives.Tests/ReactiveUI.Primitives.Tests.csproj"

# Detailed output (argument goes after --)
dotnet test "ReactiveUI.Primitives.slnx" -- --output Detailed

# List tests for a project
dotnet test "tests/ReactiveUI.Primitives.Tests/ReactiveUI.Primitives.Tests.csproj" -- --list-tests

# Fail fast
dotnet test "ReactiveUI.Primitives.slnx" -- --fail-fast
```

### TUnit `--treenode-filter` Syntax

Pattern shape:

```text
/{AssemblyName}/{Namespace}/{ClassName}/{TestMethodName}
```

Examples:

```bash
# Single test
dotnet test "tests/ReactiveUI.Primitives.Tests/ReactiveUI.Primitives.Tests.csproj" -- \
  --treenode-filter "/*/*/*/DisposableSlotsCoverAssignmentReplacementAndRemovalBranches"

# All tests in a class
dotnet test "tests/ReactiveUI.Primitives.Tests/ReactiveUI.Primitives.Tests.csproj" -- \
  --treenode-filter "/*/*/DisposableTests/*"

# All tests in a namespace
dotnet test "tests/ReactiveUI.Primitives.Tests/ReactiveUI.Primitives.Tests.csproj" -- \
  --treenode-filter "/*/ReactiveUI.Primitives.Tests/*/*"
```

If you need to target a specific project with full explicit paths:

```bash
dotnet test "tests/ReactiveUI.Primitives.Async.Tests/ReactiveUI.Primitives.Async.Tests.csproj" -- \
  --treenode-filter "/*/*/*/Async"
```

### API Approval Notes

- API approval baselines live under `src/tests/**/ApiApprovalTests.*.verified.txt`
- New TFMs usually require corresponding new `DotNet11_0.verified.txt` files
- If approval tests fail with `.received.txt` output, inspect the generated snapshot and promote it intentionally if the
  API change is expected

---

## Platform Notes

### Windows-targeted projects

This repository includes Windows-targeted TFMs and UI adapter projects:

- WPF
- WinForms
- WinUI

Non-Windows builds can still compile much of the tree because Windows targeting is enabled centrally, but some
runtime/test behavior is platform-specific.

### MAUI

This repository includes MAUI targets, including an explicit Android leg for `net11`.

Treat Android dependency installation as an environment/setup operation, not normal task guidance. Only do it when a
build proves the local machine is missing the required Android platform.

---

## Repository Conventions

- Shared target frameworks and many package conditions are centralized in `src/Directory.Build.props` and
  `src/Directory.Packages.props`
- Prefer minimal, centralized changes over per-project duplication when changing TFM policy
- Keep net11 package conditionals net11-specific; otherwise use established versions
- Prefer explicit dependency pins only when required by transitive restore behavior

---

## Test Naming And Organization

These rules are authoritative for everything under `src/tests/`.

- **Name test classes/files after the production type under test.** A test class is `<ProductionClass>Tests` (e.g.
  `SparkTests`, `WitnessTests`, `SequencerTests`, `ReplaySignalTests`). Where a cohesive family has no single umbrella
  type, name it after the namespace's representative type (e.g. `DisposableTests` for the `Disposables` family).
- **No invented, purpose-describing names.** Do not name test files after the *reason* they exist. Banned tokens in test
  file/class names unless they are literally part of a production type's name: `Coverage`, `EdgeCase`, `Edge`,
  `Contract`, `Runtime`, `Scenario`, `RealWorld`, `Patch`, `Infrastructure`, `Expansion`, `Internal`. ("Edge" is allowed
  only if the originating production class itself contains it.)
- **Group each test with the class it exercises.** When a test sits in a "coverage"-style grab-bag file, move it into
  the `<ProductionClass>Tests` file for the type it actually tests. Split a multi-subject test method by subject only
  when the split is clean; otherwise home the whole method under its dominant production type.
- **Split large test files into partial classes.** If a `<ProductionClass>Tests` file exceeds **1000 lines**, split it
  into partial-class files that group members with similar names together, suffixing by group: `FooTests.cs`,
  `FooTests.Aggregates.cs`, `FooTests.FlatMap.cs`, etc. All parts keep the same `partial class` name.
- **No `#pragma warning disable`** (see also the zero-pragma policy): fix the root cause. Long lines (S103), long
  methods (S138), and long files (S104) are fixed by wrapping/splitting, not suppressing. If a suppression is genuinely
  unavoidable, use a scoped `[SuppressMessage]` attribute, never a pragma.

---

## Agent Compatibility

If another agent entrypoint file exists, it should defer to this file.

- `AGENTS.md` is a compatibility pointer only
- `CLAUDE.md` is authoritative in this repository
