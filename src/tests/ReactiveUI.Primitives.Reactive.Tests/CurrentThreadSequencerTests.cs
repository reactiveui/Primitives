// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>
/// Verifies <see cref="CurrentThreadSequencer"/> forwards to System.Reactive's current-thread scheduler,
/// including the trampoline flag callers use to decide whether they must schedule.
/// </summary>
public class CurrentThreadSequencerTests
{
    /// <summary>Verifies the exposed instance is System.Reactive's current-thread scheduler singleton.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenInstanceRead_ThenItIsTheCurrentThreadScheduler() =>
        await Assert.That(CurrentThreadSequencer.Instance)
            .IsSameReferenceAs(System.Reactive.Concurrency.CurrentThreadScheduler.Instance);

    /// <summary>
    /// Verifies the schedule-required flag mirrors System.Reactive. Outside a trampoline both are
    /// <see langword="true"/>; the assertion compares the two rather than a literal so the test states the
    /// forwarding contract rather than a snapshot of the runtime's state.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleRequiredRead_ThenItMirrorsTheCurrentThreadScheduler() =>
        await Assert.That(CurrentThreadSequencer.IsScheduleRequired)
            .IsEqualTo(System.Reactive.Concurrency.CurrentThreadScheduler.IsScheduleRequired);

    /// <summary>Verifies the flag reports false while running inside the trampoline.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenInsideTrampoline_ThenScheduleIsNotRequired()
    {
        var observedInsideTrampoline = true;

        CurrentThreadSequencer.Instance.Schedule(() => observedInsideTrampoline = CurrentThreadSequencer.IsScheduleRequired);

        await Assert.That(observedInsideTrampoline).IsFalse();
    }
}
