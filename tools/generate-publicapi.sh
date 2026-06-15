#!/usr/bin/env bash
#
# generate-publicapi.sh — (re)generate PublicAPI baseline files for every shipped
# ReactiveUI.Primitives library, across each target framework that builds on this machine.
#
# The Microsoft.CodeAnalysis.PublicApiAnalyzers (RS0016 / RS0017 / RS0037) require a
# per-TFM pair of tracking files:
#
#     <Project>/PublicAPI/<tfm>/PublicAPI.Shipped.txt
#     <Project>/PublicAPI/<tfm>/PublicAPI.Unshipped.txt
#
# This script seeds those files and uses `dotnet format analyzers` to populate the
# Unshipped file with the project's current public surface (RS0016), drop stale
# entries (RS0017), and record nullability (RS0037).
#
# Only projects with MSBuild property TrackPublicApi=true are processed; the
# tests/ and benchmarks/ trees opt out centrally in src/Directory.Build.props.
#
# Usage:
#   tools/generate-publicapi.sh [project-name-filter]
#
# Examples:
#   tools/generate-publicapi.sh                 # all tracked libraries, all buildable TFMs
#   tools/generate-publicapi.sh Async           # only projects whose path contains 'Async'
#   tools/generate-publicapi.sh ReactiveUI.Primitives.Core
#
# Notes:
#   * Run on the OS that can build the target frameworks you need. Apple TFMs
#     (net*-ios / -maccatalyst / -tvos / -macos) build only on macOS or Windows;
#     Windows-desktop TFMs build cross-platform here via EnableWindowsTargeting.
#     Use the PowerShell sibling (generate-publicapi.ps1) on Windows.
#   * A TFM whose workload/SDK is missing is skipped with a warning (its seed files
#     are left in place) rather than aborting the whole run.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(cd "$SCRIPT_DIR/../src" && pwd)"
cd "$SRC_DIR"

# MSBuild properties that `dotnet format` cannot accept via -p:; pass through the env.
export EnableWindowsTargeting=true
export CheckEolTargetFramework=false
export MinVerVersionOverride="${MinVerVersionOverride:-255.255.255-dev}"

FILTER="${1:-}"
DIAGS="RS0016 RS0017 RS0037"

echo "PublicAPI baseline generation"
echo "  src        : $SRC_DIR"
echo "  filter     : ${FILTER:-<none>}"
echo "  diagnostics: $DIAGS"
echo "  MinVer     : $MinVerVersionOverride"
echo

projects=()
while IFS= read -r p; do projects+=("$p"); done < <(
  find . -name '*.csproj' \
    -not -path '*/tests/*' -not -path '*/benchmarks/*' \
    | sort
)

generated=0
failed=0
skipped=0
for proj in "${projects[@]}"; do
  if [ -n "$FILTER" ] && [[ "$proj" != *"$FILTER"* ]]; then continue; fi

  track="$(dotnet msbuild "$proj" -getProperty:TrackPublicApi -nologo 2>/dev/null | tr -d '[:space:]')"
  if [ "$track" != "true" ]; then
    echo "skip  (TrackPublicApi != true): $proj"
    skipped=$((skipped + 1))
    continue
  fi

  tfms="$(dotnet msbuild "$proj" -getProperty:TargetFrameworks -nologo 2>/dev/null | tr -d '[:space:]')"
  if [ -z "$tfms" ]; then
    tfms="$(dotnet msbuild "$proj" -getProperty:TargetFramework -nologo 2>/dev/null | tr -d '[:space:]')"
  fi
  if [ -z "$tfms" ]; then
    echo "skip  (no TargetFramework(s)): $proj"
    skipped=$((skipped + 1))
    continue
  fi

  projdir="$(dirname "$proj")"
  echo "=== $proj"
  echo "    TFMs: $tfms"
  IFS=';' read -ra tfm_arr <<<"$tfms"
  for tfm in "${tfm_arr[@]}"; do
    [ -z "$tfm" ] && continue
    apidir="$projdir/PublicAPI/$tfm"
    mkdir -p "$apidir"
    # Seed Shipped only if absent (preserve any genuinely shipped surface); always
    # reset Unshipped to the bare header so the regenerated surface is deterministic.
    [ -f "$apidir/PublicAPI.Shipped.txt" ] || printf '#nullable enable\n' >"$apidir/PublicAPI.Shipped.txt"
    printf '#nullable enable\n' >"$apidir/PublicAPI.Unshipped.txt"
    echo "    --> [$tfm]"
    if dotnet format analyzers "$proj" -f "$tfm" --diagnostics $DIAGS --severity info -v quiet; then
      generated=$((generated + 1))
    else
      echo "    WARN: generation failed for [$tfm] (missing workload/SDK for this platform?)"
      failed=$((failed + 1))
    fi
  done
  echo
done

echo "Done. generated: $generated TFM baseline(s), failed: $failed, projects skipped: $skipped"
[ "$failed" -eq 0 ]
