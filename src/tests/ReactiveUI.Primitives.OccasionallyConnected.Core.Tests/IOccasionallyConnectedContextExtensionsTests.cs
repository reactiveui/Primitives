// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IOccasionallyConnectedContextExtensions"/>.</summary>
public sealed class IOccasionallyConnectedContextExtensionsTests
{
    /// <summary>Verifies lifecycle convenience overloads forward the no-cancellation token.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LifecycleOverloadsForwardCancellationTokenNone()
    {
        var context = new LifecycleContext();

        await context.StartAsync();
        await context.StopAsync();

        await Assert.That(context.StartToken).IsEqualTo(CancellationToken.None);
        await Assert.That(context.StopToken).IsEqualTo(CancellationToken.None);
        await Assert.That(context.StartCalls).IsEqualTo(1);
        await Assert.That(context.StopCalls).IsEqualTo(1);
    }

    /// <summary>Verifies lifecycle convenience overloads preserve incomplete underlying work until it completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LifecycleOverloadsAwaitUnderlyingWork()
    {
        var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var context = new LifecycleContext { StartTask = startGate.Task, StopTask = stopGate.Task };

        var start = context.StartAsync();
        var stop = context.StopAsync();

        await Assert.That(start.IsCompleted).IsFalse();
        await Assert.That(stop.IsCompleted).IsFalse();
        await Assert.That(context.StartCalls).IsEqualTo(1);
        await Assert.That(context.StopCalls).IsEqualTo(1);

        startGate.SetResult();
        stopGate.SetResult();
        await start;
        await stop;
    }

    /// <summary>Verifies lifecycle convenience overloads await and preserve context failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LifecycleOverloadsPreserveAwaitedFailures()
    {
        var startError = new InvalidOperationException("start failure");
        var stopError = new InvalidOperationException("stop failure");
        var context = new LifecycleContext { StartError = startError, StopError = stopError };
        Func<Task> start = async () => await context.StartAsync();
        Func<Task> stop = async () => await context.StopAsync();

        var thrownStart = await Assert.That(start).ThrowsExactly<InvalidOperationException>();
        var thrownStop = await Assert.That(stop).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrownStart).IsSameReferenceAs(startError);
        await Assert.That(thrownStop).IsSameReferenceAs(stopError);
        await Assert.That(context.StartCalls).IsEqualTo(1);
        await Assert.That(context.StopCalls).IsEqualTo(1);
    }

    /// <summary>Records context lifecycle operations.</summary>
    private sealed class LifecycleContext : IOccasionallyConnectedContext
    {
        /// <summary>Gets the number of start calls.</summary>
        public int StartCalls { get; private set; }

        /// <summary>Gets the number of stop calls.</summary>
        public int StopCalls { get; private set; }

        /// <summary>Gets the token supplied to start.</summary>
        public CancellationToken StartToken { get; private set; }

        /// <summary>Gets the token supplied to stop.</summary>
        public CancellationToken StopToken { get; private set; }

        /// <summary>Gets or sets the error returned by start.</summary>
        public Exception? StartError { get; init; }

        /// <summary>Gets or sets the error returned by stop.</summary>
        public Exception? StopError { get; init; }

        /// <summary>Gets or sets the incomplete task returned by start.</summary>
        public Task? StartTask { get; init; }

        /// <summary>Gets or sets the incomplete task returned by stop.</summary>
        public Task? StopTask { get; init; }

        /// <inheritdoc />
        public ISyncEngine SyncEngine => throw new NotSupportedException();

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates => throw new NotSupportedException();

        /// <inheritdoc />
        public IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(StreamDefinition<TState, TInput> definition) => throw new NotSupportedException();

        /// <inheritdoc />
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            StartCalls++;
            StartToken = cancellationToken;
            return CreateLifecycleTask(StartError, StartTask);
        }

        /// <inheritdoc />
        public ValueTask StopAsync(CancellationToken cancellationToken)
        {
            StopCalls++;
            StopToken = cancellationToken;
            return CreateLifecycleTask(StopError, StopTask);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        /// <summary>Creates the lifecycle result for a configured error, asynchronous task, or immediate completion.</summary>
        /// <param name="error">The configured error.</param>
        /// <param name="task">The configured asynchronous task.</param>
        /// <returns>The lifecycle result.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ValueTask CreateLifecycleTask(Exception? error, Task? task)
        {
            if (error is not null)
            {
                return ValueTask.FromException(error);
            }

            return task is null ? ValueTask.CompletedTask : new(task);
        }
    }
}
