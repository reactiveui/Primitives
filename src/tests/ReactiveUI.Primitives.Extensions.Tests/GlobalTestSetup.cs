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
    /// <summary>The worker-thread floor for the test process; see <see cref="ConfigureDefaults"/>.</summary>
    private const int MinPoolThreads = 32;

    /// <summary>
    /// Caps every test at 60 seconds and raises the thread-pool worker floor before any test runs.
    /// <para>
    /// The 60s cap: without a default, a single flaky test that hangs stalls the entire assembly (the
    /// whole suite is serialised via <c>[assembly: NotInParallel(nameof(UnhandledExceptionHandler))]</c>)
    /// and we lose the per-test failure signal — CI just reports the workflow-level timeout. 60s is far
    /// above every legitimate test (slowest non-cancellation test is ~5s) so any future hang fails its
    /// own test with a clear message instead of killing the whole run.
    /// </para>
    /// <para>
    /// The thread-pool floor: several tests rendezvous two blocking work items on the default pool
    /// (e.g. <c>Continuation</c>'s <c>Barrier.SignalAndWait</c>, scheduled retries). A busy CI runner can
    /// saturate the pool, which only hill-climbs new workers ~1/sec, so a rendezvous needing a second
    /// thread can miss its guard window and time out. Raising the floor makes those threads available
    /// immediately rather than starved behind unrelated work.
    /// </para>
    /// </summary>
    /// <param name="context">The TUnit test-discovery context exposing programmatic settings.</param>
    [Before(HookType.TestDiscovery)]
    public static void ConfigureDefaults(BeforeTestDiscoveryContext context)
    {
        context.Settings.Timeouts.DefaultTestTimeout = TimeSpan.FromSeconds(60);

        ThreadPool.GetMinThreads(out var workerThreads, out var completionPortThreads);
        _ = ThreadPool.SetMinThreads(
            Math.Max(workerThreads, MinPoolThreads),
            Math.Max(completionPortThreads, MinPoolThreads));
    }
}
