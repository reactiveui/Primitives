// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Verifies <see cref="CurrentThreadSequencer"/> forwards to System.Reactive's current-thread scheduler, trampoline flag included.</summary>
public class CurrentThreadSequencerTests
{
    /// <summary>Verifies the exposed instance is System.Reactive's current-thread scheduler singleton.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenInstanceRead_ThenItIsTheCurrentThreadScheduler() =>
        await Assert.That(CurrentThreadSequencer.Instance)
            .IsSameReferenceAs(System.Reactive.Concurrency.CurrentThreadScheduler.Instance);

    /// <summary>Verifies the schedule-required flag mirrors System.Reactive's.</summary>
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
        StrongBox<bool> observedInsideTrampoline = new(true);

        _ = CurrentThreadSequencer.Instance.Schedule(
            observedInsideTrampoline,
            static (_, state) =>
            {
                state.Value = CurrentThreadSequencer.IsScheduleRequired;
                return Disposable.Empty;
            });

        await Assert.That(observedInsideTrampoline.Value).IsFalse();
    }
}
