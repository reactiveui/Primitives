#!/usr/bin/env bash
#
# generate-publicapi.sh — (re)generate PublicAPI baseline files for every shipped
# ReactiveUI library, across each target framework that builds on this machine.
#
# The Microsoft.CodeAnalysis.PublicApiAnalyzers (RS0016 / RS0017 / RS0037) require a
# per-TFM pair of tracking files:
#
#     <Project>/PublicAPI/<tfm>/PublicAPI.Shipped.txt
#     <Project>/PublicAPI/<tfm>/PublicAPI.Unshipped.txt
#
# This script preserves the existing Shipped baseline, resets only Unshipped, then
# uses `dotnet format analyzers` to add missing public API entries (RS0016), drop
# stale shipped entries (RS0017), and record nullability (RS0037). The resulting
# Shipped and Unshipped files are folded back into Shipped, leaving Unshipped empty.
#
# Tests, benchmarks, and source generators are skipped structurally.
#
# Projects run in parallel through a bounded pool, while TFMs within one project run
# serially. `dotnet format` can retain sibling-TFM AdditionalFiles as memory-mapped
# inputs, so parallel TFMs from the same project are unsafe. The formatter can also
# simplify source imports while applying analyzer fixes; exact pre-run source bytes are
# restored on exit. Override project concurrency with JOBS=<n>.
#
# Usage:
#   tools/generate-publicapi.sh [project-name-filter]
#
# Examples:
#   tools/generate-publicapi.sh                 # all tracked libraries, all buildable TFMs
#   tools/generate-publicapi.sh Async           # only projects whose path contains 'Async'
#   JOBS=4 tools/generate-publicapi.sh          # cap parallelism at 4
#
# Notes:
#   * Run on the OS that can build the target frameworks you need. Apple TFMs
#     (net*-ios / -maccatalyst / -tvos / -macos) build only on macOS or Windows;
#     Windows-desktop TFMs build cross-platform here via EnableWindowsTargeting.
#     Use the PowerShell sibling (generate-publicapi.ps1) on Windows.
#   * A TFM whose workload/SDK is missing is reported as failed (its seed files are
#     left in place) rather than aborting the whole run; the exit code is non-zero so
#     CI can detect an incomplete run.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(cd "$SCRIPT_DIR/../src" && pwd)"
cd "$SRC_DIR"

# MSBuild properties that `dotnet format` cannot accept via -p:; pass through the env.
export EnableWindowsTargeting=true
export CheckEolTargetFramework=false
export MinVerVersionOverride="${MinVerVersionOverride:-255.255.255-dev}"
export DOTNET_CLI_TELEMETRY_OPTOUT=1
export DOTNET_NOLOGO=true
export DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
# The generator must not inherit a shell/MSBuild opt-out. Force API tracking on for
# normal library projects; Directory.Build.props still disables tests, benchmarks,
# and source generators by project path/name.
export TrackPublicApi=true
unset TRACKPUBLICAPI trackpublicapi

FILTER="${1:-}"
export DIAGS="RS0016 RS0017 RS0037"
JOBS="${JOBS:-$(nproc 2>/dev/null || echo 4)}"
[ "$JOBS" -gt 8 ] && JOBS=8

echo "PublicAPI baseline generation"
echo "  src        : $SRC_DIR"
echo "  filter     : ${FILTER:-<none>}"
echo "  diagnostics: $DIAGS"
echo "  MinVer     : $MinVerVersionOverride"
echo "  jobs       : $JOBS"
echo

get_msbuild_property() {
  dotnet msbuild "$1" "-getProperty:$2" -nologo 2>/dev/null \
    | awk 'NF { value = $0 } END { print value }'
}

projects=()
while IFS= read -r p; do projects+=("$p"); done < <(
  find . -name '*.csproj' \
    -not -path '*/tests/*' -not -path '*/benchmarks/*' \
    -not -name '*.Generator.csproj' \
    | sort
)

# Collect (project|tfm) work items; the worker seeds, generates, and folds each pair.
items=()
restore_set=()
skipped=0
for proj in "${projects[@]}"; do
  if [ -n "$FILTER" ] && [[ "$proj" != *"$FILTER"* ]]; then continue; fi

  tfms="$(get_msbuild_property "$proj" TargetFrameworks | tr -d '[:space:]')"
  if [ -z "$tfms" ]; then
    tfms="$(get_msbuild_property "$proj" TargetFramework | tr -d '[:space:]')"
  fi
  if [ -z "$tfms" ]; then
    echo "skip  (no TargetFramework(s)): $proj"
    skipped=$((skipped + 1))
    continue
  fi

  projdir="$(dirname "$proj")"
  api_namespace="$(get_msbuild_property "$proj" RootNamespace | tr -d '[:space:]')"
  if [ -z "$api_namespace" ]; then
    api_namespace="$(get_msbuild_property "$proj" AssemblyName | tr -d '[:space:]')"
  fi
  echo "queue $proj"
  echo "    TFMs: $tfms"
  restore_set+=("$proj")
  IFS=';' read -ra tfm_arr <<<"$tfms"
  for tfm in "${tfm_arr[@]}"; do
    [ -z "$tfm" ] && continue
    mkdir -p "$projdir/PublicAPI/$tfm"
    items+=("$proj|$tfm|$api_namespace")
  done
done
echo

if [ "${#items[@]}" -eq 0 ]; then
  echo "Nothing to generate. projects skipped: $skipped"
  exit 0
fi

# Restore once per project so the parallel `dotnet format` workers never race on restore
# (they each load a read-only workspace afterwards).
echo "Restoring ${#restore_set[@]} project(s)..."
for proj in "${restore_set[@]}"; do
  dotnet restore "$proj" -v quiet || echo "    WARN: restore reported issues for $proj"
done
echo

write_header() {
  printf '#nullable enable\n' >"$1"
}

ensure_api_file() {
  local file="$1"
  if [ ! -f "$file" ]; then
    write_header "$file"
    return
  fi

  if ! grep -qxF '#nullable enable' "$file"; then
    local tmp="$RESULTS_DIR/ensure.$RANDOM.$RANDOM"
    {
      printf '#nullable enable\n'
      grep -v '^[[:space:]]*$' "$file"
    } >"$tmp"
    mv "$tmp" "$file"
  fi
}

api_line_count() {
  if [ ! -f "$1" ]; then
    printf '0\n'
    return
  fi

  grep -vxF '#nullable enable' "$1" | grep -vc '^[[:space:]]*$' || true
}

merge_api_files() {
  local output="$1"
  local shipped="$2"
  local unshipped="$3"
  local tfm="$4"
  local api_namespace="$5"
  {
    printf '#nullable enable\n'
    {
      grep -vxF '#nullable enable' "$shipped" 2>/dev/null || true
      grep -vxF '#nullable enable' "$unshipped" 2>/dev/null || true
      if [[ "$tfm" == *-android* ]] && [ -n "$api_namespace" ]; then
        printf '%s.Resource\n' "$api_namespace"
        printf '%s.Resource.Resource() -> void\n' "$api_namespace"
      fi
    } | grep -v '^[[:space:]]*$' | LC_ALL=C sort -u
  } >"$output"
}

# Worker: update one (project, TFM) pair and fold missing Unshipped entries into Shipped.
generate_one() {
  local item="$1"
  local proj tfm api_namespace
  IFS='|' read -r proj tfm api_namespace <<<"$item"
  local projdir apidir shipped unshipped tag
  projdir="$(dirname "$proj")"
  apidir="$projdir/PublicAPI/$tfm"
  shipped="$apidir/PublicAPI.Shipped.txt"
  unshipped="$apidir/PublicAPI.Unshipped.txt"
  tag="$(printf '%s' "$item" | tr '/|.' '___')"
  local bsh="$RESULTS_DIR/$tag.shipped.bak"
  local bun="$RESULTS_DIR/$tag.unshipped.bak"
  # Back up any existing baseline so a build failure (e.g. a TFM that needs a workload
  # this platform lacks) restores it instead of wiping it.
  [ -f "$shipped" ] && cp "$shipped" "$bsh"
  [ -f "$unshipped" ] && cp "$unshipped" "$bun"

  ensure_api_file "$shipped"
  local before_count
  before_count="$(api_line_count "$shipped")"
  # Keep Shipped as input so RS0016 reports only missing symbols. Reset Unshipped so
  # the generated delta is explicit and safe to fold after a successful analyzer run.
  write_header "$unshipped"

  if dotnet format analyzers "$proj" -f "$tfm" --diagnostics $DIAGS --severity info -v quiet; then
    local merged="$RESULTS_DIR/$tag.merged"
    merge_api_files "$merged" "$shipped" "$unshipped" "$tfm" "$api_namespace"
    local after_count
    after_count="$(api_line_count "$merged")"
    if [ "$before_count" -gt 0 ] && [ "$after_count" -eq 0 ]; then
      [ -f "$bsh" ] && cp "$bsh" "$shipped"
      [ -f "$bun" ] && cp "$bun" "$unshipped"
      printf 'FAIL [%s] %s (refusing to replace non-empty Shipped with an empty baseline)\n' "$tfm" "$proj"
      : >"$RESULTS_DIR/$tag.fail"
      return
    fi

    mv "$merged" "$shipped"
    write_header "$unshipped"
    printf 'OK   [%s] %s\n' "$tfm" "$proj"
    : >"$RESULTS_DIR/$tag.ok"
  else
    # Restore the prior baseline (if any) so nothing is wiped for a TFM we can't build here.
    [ -f "$bsh" ] && cp "$bsh" "$shipped"
    [ -f "$bun" ] && cp "$bun" "$unshipped"
    printf 'FAIL [%s] %s (missing workload/SDK for this platform?)\n' "$tfm" "$proj"
    : >"$RESULTS_DIR/$tag.fail"
  fi
}
export -f generate_one
export -f write_header
export -f ensure_api_file
export -f api_line_count
export -f merge_api_files

RESULTS_DIR="$(mktemp -d)"
export RESULTS_DIR

REPO_DIR="$(cd "$SRC_DIR/.." && pwd)"
SOURCE_BACKUP="$RESULTS_DIR/source-backup"
ITEMS_FILE="$RESULTS_DIR/items"
export SOURCE_BACKUP ITEMS_FILE REPO_DIR
mkdir -p "$SOURCE_BACKUP"

# Preserve exact source bytes, including intentional dirty-worktree changes. Analyzer
# fixes should update only PublicAPI AdditionalFiles, but dotnet-format may also simplify
# source imports as a side effect.
while IFS= read -r -d '' source; do
  rel="${source#"$REPO_DIR/"}"
  mkdir -p "$SOURCE_BACKUP/$(dirname "$rel")"
  cp "$source" "$SOURCE_BACKUP/$rel"
done < <(find "$SRC_DIR" -type f -name '*.cs' ! -path '*/bin/*' ! -path '*/obj/*' -print0)

restore_sources() {
  local restored=0 backup rel target
  while IFS= read -r -d '' backup; do
    rel="${backup#$SOURCE_BACKUP/}"
    target="$REPO_DIR/$rel"
    if [ -f "$target" ] && ! cmp -s "$backup" "$target"; then
      cp "$backup" "$target"
      restored=$((restored + 1))
    fi
  done < <(find "$SOURCE_BACKUP" -type f -print0)
  printf '%s' "$restored" >"$RESULTS_DIR/source-files-restored"
}

trap 'restore_sources; rm -rf "$RESULTS_DIR"' EXIT

printf '%s\n' "${items[@]}" >"$ITEMS_FILE"

generate_project() {
  local project="$1" item item_project
  while IFS= read -r item; do
    IFS='|' read -r item_project _ <<<"$item"
    if [ "$item_project" = "$project" ]; then
      generate_one "$item"
    fi
  done <"$ITEMS_FILE"
}
export -f generate_project

echo "Generating ${#items[@]} (project, TFM) baseline(s) across $JOBS job(s)..."
printf '%s\0' "${restore_set[@]}" | xargs -0 -P "$JOBS" -I{} bash -c 'generate_project "$1"' _ "{}"
echo

generated="$(find "$RESULTS_DIR" -name '*.ok' | wc -l | tr -d '[:space:]')"
failed="$(find "$RESULTS_DIR" -name '*.fail' | wc -l | tr -d '[:space:]')"
restore_sources
restored_sources="$(cat "$RESULTS_DIR/source-files-restored")"

echo "Done. generated: $generated TFM baseline(s), failed: $failed, projects skipped: $skipped, source files restored: $restored_sources"
[ "$failed" -eq 0 ]
