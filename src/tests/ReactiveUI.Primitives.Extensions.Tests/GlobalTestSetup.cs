// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// Configures assembly-wide TUnit defaults via a <see cref="TestDiscovery"/> hook so they
/// are in place before any test executes.
/// </summary>
internal static class GlobalTestSetup
{
    /// <summary>
    /// Caps every test at 60 seconds. Without a default cap, a single flaky test that hangs
    /// stalls the entire assembly (the whole suite is serialised via
    /// <c>[assembly: NotInParallel(nameof(UnhandledExceptionHandler))]</c>) and we lose the
    /// per-test failure signal — the CI just reports the workflow-level timeout. 60s is far
    /// above every legitimate test (slowest non-cancellation test is ~5s) so any future hang
    /// fails its own test with a clear message instead of killing the whole run.
    /// </summary>
    /// <param name="context">The TUnit test-discovery context exposing programmatic settings.</param>
    [Before(HookType.TestDiscovery)]
    public static void ConfigureDefaults(BeforeTestDiscoveryContext context) =>
        context.Settings.Timeouts.DefaultTestTimeout = TimeSpan.FromSeconds(60);
}
