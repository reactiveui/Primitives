# GitHub Copilot Instructions for ReactiveUI.Primitives

## Overview
This repository contains ReactiveUI.Primitives, a focused collection of high-performance reactive primitives for .NET applications.

## Required Pre-Steps for Linux Builds

Before performing any build, test, or development tasks on Linux, you **MUST** complete the following steps in order:

### 1. Install .NET SDK
Ensure that .NET 8, 9, and/or 10 are installed:
```bash
# Check installed versions
dotnet --list-sdks

# If any required version is missing, use the dotnet-install script
# Note: Microsoft changed the install script domain from dot.net to dotnet.microsoft.com
# See: https://devblogs.microsoft.com/dotnet/critical-dotnet-install-links-are-changing/

# Download and run the install script (for Linux/macOS)
curl -sSL https://dotnet.microsoft.com/download/dotnet/scripts/v1/dotnet-install.sh | bash /dev/stdin --channel 8.0
curl -sSL https://dotnet.microsoft.com/download/dotnet/scripts/v1/dotnet-install.sh | bash /dev/stdin --channel 9.0
curl -sSL https://dotnet.microsoft.com/download/dotnet/scripts/v1/dotnet-install.sh | bash /dev/stdin --channel 10.0

# Or download the script first and run it
# wget https://dotnet.microsoft.com/download/dotnet/scripts/v1/dotnet-install.sh
# chmod +x dotnet-install.sh
# ./dotnet-install.sh --channel 8.0
# ./dotnet-install.sh --channel 9.0
# ./dotnet-install.sh --channel 10.0

# For more details, see: https://learn.microsoft.com/en-us/dotnet/core/tools/dotnet-install-script
```

### 2. Unshallow the Git Repository
The repository uses MinVer, which relies on git history for version height. If the repository is shallow (typical in CI/CD environments), unshallow it:

```bash
# Check if repository is shallow
git rev-parse --is-shallow-repository

# If true, unshallow the repository
git fetch --unshallow
```

**This step is critical** - builds will fail without the full git history.

### 3. Restore Workloads
Navigate to the `/src` folder and restore required workloads:

```bash
cd /home/runner/work/Primitives/Primitives/src
dotnet workload restore
```

### 4. Restore Dependencies
Still in the `/src` folder, restore all project dependencies:

```bash
dotnet restore
```

## Build Instructions
After completing the pre-steps above:

```bash
cd /home/runner/work/Primitives/Primitives/src
dotnet build ReactiveUI.Primitives.slnx
```

## Test Instructions
Run tests with:

```bash
cd /home/runner/work/Primitives/Primitives/src
dotnet test ReactiveUI.Primitives.slnx
```

## Project Structure
- `/src` - Main source code directory
  - `ReactiveUI.Primitives/` - Main library project
  - `tests/ReactiveUI.Primitives.Tests/` - Test project
  - `benchmarks/ReactiveUI.Primitives.Benchmarks/` - Benchmark project
  - `ReactiveUI.Primitives.slnx` - Solution file

## Important Notes
- Target frameworks: net462, net472, net481, .NET 8, .NET 9, .NET 10
- Uses StyleCop and Roslynator analyzers for code quality
- Nullable reference types are enabled
- Documentation XML files are generated
- Uses MinVer for versioning
