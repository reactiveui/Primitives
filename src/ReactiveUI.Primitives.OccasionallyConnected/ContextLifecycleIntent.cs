// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores context lifecycle intent while the owning context gate is held.</summary>
internal sealed class ContextLifecycleIntent
{
    /// <summary>Stores the context-owned cancellation generation for the active start interval.</summary>
    private StartGeneration? _startGeneration;

    /// <summary>Tracks whether the context has completed startup and has not stopped.</summary>
    private bool _running;

    /// <summary>Tracks whether stop or dispose has requested cancellation before or during startup.</summary>
    private bool _stopRequested;

    /// <summary>Stores the shared accepted stop task while stop cleanup is running.</summary>
    private Task? _stopTask;

    /// <summary>Gets whether the context has completed startup and has not stopped.</summary>
    internal bool IsRunning => _running;

    /// <summary>Gets the accepted stop task currently known to the context.</summary>
    /// <returns>The accepted stop task, or <see langword="null"/> when no stop was accepted.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Task? SnapshotAcceptedStopTask() => _stopTask;

    /// <summary>Accepts a stop operation or returns the already accepted stop task.</summary>
    /// <returns>The accepted stop decision.</returns>
    internal AcceptedStopDecision TryAcceptStop()
    {
        if (_stopTask is null || _stopTask.IsCompleted)
        {
            var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _stopTask = completion.Task;
            return new(_stopTask, completion);
        }

        return new(_stopTask, Completion: null);
    }

    /// <summary>Begins a shared start attempt and returns its context-owned cancellation generation.</summary>
    /// <returns>The generation lease and whether cancellation must launch outside the owning gate.</returns>
    internal StartGenerationLease BeginStart()
    {
        var generation = new StartGeneration();
        _startGeneration = generation;
        _running = false;
        if (!_stopRequested)
        {
            return new(generation, LaunchCancellation: false);
        }

        generation.RecordStop();
        return new(generation, LaunchCancellation: true);
    }

    /// <summary>Cancels an active start attempt after the owning gate is released.</summary>
    /// <returns>The shared cancellation decision.</returns>
    internal StartCancellationDecision CancelStart()
    {
        _stopRequested = true;
        if (_startGeneration is not { } generation)
        {
            return new(Task.CompletedTask, GenerationToCancel: null);
        }

        if (generation.StopRequested)
        {
            return new(generation.CancellationTask, GenerationToCancel: null);
        }

        generation.RecordStop();
        return new(generation.CancellationTask, generation);
    }

    /// <summary>Clears stale accepted-stop intent when no start generation owns it.</summary>
    internal void ClearAcceptedStopIntent()
    {
        if (_startGeneration is null)
        {
            _stopRequested = false;
        }
    }

    /// <summary>Completes a start generation and atomically captures its cancellation drain before detaching it.</summary>
    /// <param name="generation">The completed generation.</param>
    /// <returns>The detached generation lease.</returns>
    internal StartCancellationLease CompleteStartGeneration(StartGeneration generation)
    {
        _running = false;
        _stopRequested = false;
        if (ReferenceEquals(_startGeneration, generation))
        {
            _startGeneration = null;
        }

        return new(
            generation,
            generation.StopRequested ? generation.CancellationTask : Task.CompletedTask);
    }

    /// <summary>Marks the context as not running after stop or disposal.</summary>
    /// <returns>The generation to dispose after active users have observed cancellation.</returns>
    internal StartGeneration? MarkNotRunning()
    {
        var completed = _startGeneration;
        _running = false;
        _stopRequested = false;
        _startGeneration = null;
        return completed;
    }

    /// <summary>Gets whether a late stream start can be admitted to the current running generation.</summary>
    /// <returns>The late stream start decision.</returns>
    internal LateStreamStartDecision CanStartLateStream() =>
        _running && !_stopRequested && _startGeneration is { } generation
            ? new(CanStart: true, generation)
            : new(CanStart: false, Generation: null);

    /// <summary>Attempts to commit a completed start sweep as the running generation.</summary>
    /// <param name="generation">The start generation being committed.</param>
    /// <param name="registrationChanged">Whether registrations changed after the sweep snapshot.</param>
    /// <returns><see langword="true"/> when the generation committed; otherwise <see langword="false"/> to retry.</returns>
    /// <exception cref="OperationCanceledException">A stop or disposal intent canceled the generation before commit.</exception>
    internal bool TryCommitRunning(StartGeneration generation, bool registrationChanged)
    {
        if (registrationChanged)
        {
            return false;
        }

        if (!ReferenceEquals(_startGeneration, generation)
            || _stopRequested
            || generation.StopRequested
            || generation.Token.IsCancellationRequested)
        {
            throw new OperationCanceledException(generation.Token);
        }

        _running = true;
        return true;
    }
}
