// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="ContextLifecycleIntent"/>.</summary>
public sealed class ContextLifecycleIntentTests
{
    /// <summary>The timeout used for direct lifecycle intent assertions.</summary>
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies accepted stop intent recorded before startup cancels the next generation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CancelStartBeforeBeginStartMarksNextGenerationForCancellation()
    {
        var intent = new ContextLifecycleIntent();
        var cancellation = intent.CancelStart();
        var begin = intent.BeginStart();

        try
        {
            await Assert.That(cancellation.CancellationTask).IsSameReferenceAs(Task.CompletedTask);
            await Assert.That(cancellation.LaunchCancellation).IsFalse();
            await Assert.That(begin.Generation.StopRequested).IsTrue();
            await Assert.That(begin.LaunchCancellation).IsTrue();
        }
        finally
        {
            await DrainAndDisposeAsync(begin.Generation, begin);
        }
    }

    /// <summary>Verifies repeated stop requests share one generation drain and launch cancellation once.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CancelStartWithActiveGenerationReturnsSharedDrainAndLaunchesOnce()
    {
        var intent = new ContextLifecycleIntent();
        var begin = intent.BeginStart();
        var first = intent.CancelStart();
        var second = intent.CancelStart();

        try
        {
            await Assert.That(first.CancellationTask).IsSameReferenceAs(begin.Generation.CancellationTask);
            await Assert.That(second.CancellationTask).IsSameReferenceAs(begin.Generation.CancellationTask);
            await Assert.That(first.LaunchCancellation).IsTrue();
            await Assert.That(second.LaunchCancellation).IsFalse();
        }
        finally
        {
            await DrainAndDisposeAsync(begin.Generation, first, second);
        }
    }

    /// <summary>Verifies accepted stop cleanup cannot clear intent that an active generation owns.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ClearAcceptedStopIntentDoesNotClearIntentOwnedByActiveGeneration()
    {
        var intent = new ContextLifecycleIntent();
        var begin = intent.BeginStart();
        var cancellation = intent.CancelStart();

        try
        {
            intent.ClearAcceptedStopIntent();
            var exception = await Assert.That(() => intent.TryCommitRunning(begin.Generation, registrationChanged: false))
                .ThrowsExactly<OperationCanceledException>();

            await Assert.That(exception).IsNotNull();
            await Assert.That(exception?.CancellationToken).IsEqualTo(begin.Generation.Token);
            await Assert.That(intent.IsRunning).IsFalse();

            _ = intent.CompleteStartGeneration(begin.Generation);
            intent.ClearAcceptedStopIntent();
            await AssertFreshGenerationCanCommitAsync(intent);
        }
        finally
        {
            await DrainAndDisposeAsync(begin.Generation, cancellation);
        }
    }

    /// <summary>Verifies failed-start cleanup detaches the generation while preserving its recorded cancellation drain.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CompleteStartGenerationPreservesPreviouslyRecordedCancellationDrain()
    {
        var intent = new ContextLifecycleIntent();
        var begin = intent.BeginStart();
        var cancellation = intent.CancelStart();

        try
        {
            var lease = intent.CompleteStartGeneration(begin.Generation);

            await Assert.That(lease.Generation).IsSameReferenceAs(begin.Generation);
            await Assert.That(lease.CancellationTask).IsSameReferenceAs(begin.Generation.CancellationTask);
            await Assert.That(intent.CanStartLateStream().CanStart).IsFalse();
        }
        finally
        {
            await DrainAndDisposeAsync(begin.Generation, cancellation);
        }
    }

    /// <summary>Verifies a stop intent after a stream sweep rejects the final running commit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TryCommitRunningRejectsStopIntentAfterStreamSweep()
    {
        var intent = new ContextLifecycleIntent();
        var begin = intent.BeginStart();
        var cancellation = intent.CancelStart();

        try
        {
            var exception = await Assert.That(() => intent.TryCommitRunning(begin.Generation, registrationChanged: false))
                .ThrowsExactly<OperationCanceledException>();

            await Assert.That(exception).IsNotNull();
            await Assert.That(exception?.CancellationToken).IsEqualTo(begin.Generation.Token);
            await Assert.That(intent.IsRunning).IsFalse();
        }
        finally
        {
            await DrainAndDisposeAsync(begin.Generation, cancellation);
        }
    }

    /// <summary>Verifies a clean start commits running state when no stop or registration change intervenes.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TryCommitRunningCommitsWhenNoStopIntentExists()
    {
        var intent = new ContextLifecycleIntent();
        var begin = intent.BeginStart();

        try
        {
            var result = intent.TryCommitRunning(begin.Generation, registrationChanged: false);
            var candidate = intent.CanStartLateStream();

            await Assert.That(result).IsTrue();
            await Assert.That(intent.IsRunning).IsTrue();
            await Assert.That(candidate.CanStart).IsTrue();
            await Assert.That(candidate.Generation).IsSameReferenceAs(begin.Generation);
        }
        finally
        {
            await DrainRunningGenerationAsync(intent, begin.Generation);
        }
    }

    /// <summary>Verifies registration changes request a retry without committing or discarding the generation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TryCommitRunningRetriesWhenRegistrationChanges()
    {
        var intent = new ContextLifecycleIntent();
        var begin = intent.BeginStart();

        try
        {
            var retry = intent.TryCommitRunning(begin.Generation, registrationChanged: true);
            var commit = intent.TryCommitRunning(begin.Generation, registrationChanged: false);

            await Assert.That(retry).IsFalse();
            await Assert.That(commit).IsTrue();
            await Assert.That(intent.IsRunning).IsTrue();
        }
        finally
        {
            await DrainRunningGenerationAsync(intent, begin.Generation);
        }
    }

    /// <summary>Verifies late stream starts are rejected once stop intent is pending.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CanStartLateStreamRejectsWhenStopIntentPending()
    {
        var intent = new ContextLifecycleIntent();
        var begin = intent.BeginStart();
        StartCancellationDecision? cancellation = null;

        try
        {
            var commit = intent.TryCommitRunning(begin.Generation, registrationChanged: false);
            cancellation = intent.CancelStart();
            var candidate = intent.CanStartLateStream();

            await Assert.That(commit).IsTrue();
            await Assert.That(candidate.CanStart).IsFalse();
            await Assert.That(candidate.Generation).IsNull();
        }
        finally
        {
            if (cancellation is { } decision)
            {
                await DrainRunningGenerationAsync(intent, begin.Generation, decision);
            }
            else
            {
                await DrainRunningGenerationAsync(intent, begin.Generation);
            }
        }
    }

    /// <summary>Verifies a fresh generation can commit after completed stop cleanup clears old intent.</summary>
    /// <param name="intent">The lifecycle intent to verify.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertFreshGenerationCanCommitAsync(ContextLifecycleIntent intent)
    {
        var fresh = intent.BeginStart();

        try
        {
            await Assert.That(fresh.Generation.StopRequested).IsFalse();
            await Assert.That(fresh.LaunchCancellation).IsFalse();
            await Assert.That(intent.TryCommitRunning(fresh.Generation, registrationChanged: false)).IsTrue();
            await Assert.That(intent.IsRunning).IsTrue();
        }
        finally
        {
            await DrainRunningGenerationAsync(intent, fresh.Generation);
        }
    }

    /// <summary>Drains a running generation, then releases it.</summary>
    /// <param name="intent">The lifecycle intent that owns the generation.</param>
    /// <param name="generation">The start generation to release.</param>
    /// <returns>A task representing the drain.</returns>
    private static async Task DrainRunningGenerationAsync(ContextLifecycleIntent intent, StartGeneration generation)
    {
        var detached = intent.MarkNotRunning();
        await DrainDetachedOrOriginalAsync(detached, generation);
    }

    /// <summary>Drains a running generation after launching the accepted cancellation.</summary>
    /// <param name="intent">The lifecycle intent that owns the generation.</param>
    /// <param name="generation">The start generation to release.</param>
    /// <param name="decision">The cancellation launch decision to honor.</param>
    /// <returns>A task representing the drain.</returns>
    private static async Task DrainRunningGenerationAsync(
        ContextLifecycleIntent intent,
        StartGeneration generation,
        StartCancellationDecision decision)
    {
        decision.LaunchCancellationIfNeeded();
        var detached = intent.MarkNotRunning();
        await DrainDetachedOrOriginalAsync(detached, generation);
    }

    /// <summary>Drains the generation detached from running state, or the original if it was not detached.</summary>
    /// <param name="detached">The detached running generation.</param>
    /// <param name="original">The original start generation.</param>
    /// <returns>A task representing the drain.</returns>
    private static async Task DrainDetachedOrOriginalAsync(StartGeneration? detached, StartGeneration original)
    {
        if (detached is null)
        {
            await DrainAndDisposeAsync(original);
            return;
        }

        await DrainAndDisposeAsync(detached);
    }

    /// <summary>Drains requested cancellation and releases a start generation.</summary>
    /// <param name="generation">The start generation to release.</param>
    /// <returns>A task representing the drain.</returns>
    private static async Task DrainAndDisposeAsync(StartGeneration generation)
    {
        try
        {
            if (generation.StopRequested)
            {
                await generation.CancellationTask.WaitAsync(TestTimeout);
            }
        }
        finally
        {
            generation.Dispose();
        }
    }

    /// <summary>Drains requested cancellation and releases a start generation.</summary>
    /// <param name="generation">The start generation to release.</param>
    /// <param name="lease">The generation cancellation launch lease to honor.</param>
    /// <returns>A task representing the drain.</returns>
    private static async Task DrainAndDisposeAsync(StartGeneration generation, StartGenerationLease lease)
    {
        lease.LaunchCancellationIfNeeded();
        await DrainAndDisposeAsync(generation);
    }

    /// <summary>Drains requested cancellation and releases a start generation.</summary>
    /// <param name="generation">The start generation to release.</param>
    /// <param name="decision">The cancellation launch decision to honor.</param>
    /// <returns>A task representing the drain.</returns>
    private static async Task DrainAndDisposeAsync(StartGeneration generation, StartCancellationDecision decision)
    {
        decision.LaunchCancellationIfNeeded();
        await DrainAndDisposeAsync(generation);
    }

    /// <summary>Drains requested cancellation and releases a start generation.</summary>
    /// <param name="generation">The start generation to release.</param>
    /// <param name="first">The first cancellation launch decision to honor.</param>
    /// <param name="second">The second cancellation launch decision to honor.</param>
    /// <returns>A task representing the drain.</returns>
    private static async Task DrainAndDisposeAsync(
        StartGeneration generation,
        StartCancellationDecision first,
        StartCancellationDecision second)
    {
        first.LaunchCancellationIfNeeded();
        second.LaunchCancellationIfNeeded();
        await DrainAndDisposeAsync(generation);
    }
}
