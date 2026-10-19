// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Observer helpers for <see cref="CollaborationClientApplicationTests"/>.</summary>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>Owns bounded subscriptions for one client activity stream.</summary>
    private sealed class ActivityTelemetry : IDisposable
    {
        /// <summary>The owned subscriptions.</summary>
        private readonly IDisposable[] _subscriptions;

        /// <summary>Initializes a new instance of the <see cref="ActivityTelemetry"/> class.</summary>
        /// <param name="activity">The activity stream.</param>
        internal ActivityTelemetry(IOccasionallyConnectedStream<ActivityView, ActivityUpdate> activity)
            : this(activity, TimeProvider.System)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="ActivityTelemetry"/> class.</summary>
        /// <param name="activity">The activity stream.</param>
        /// <param name="timeProvider">The clock used for telemetry timestamps.</param>
        internal ActivityTelemetry(
            IOccasionallyConnectedStream<ActivityView, ActivityUpdate> activity,
            TimeProvider timeProvider)
        {
            ArgumentNullException.ThrowIfNull(timeProvider);

            Local = new(TelemetryCapacity);
            Remote = new(TelemetryCapacity);
            Sync = new(TelemetryCapacity);
            Operations = new(TelemetryCapacity);
            Faults = new(TelemetryCapacity);
            CreatedAtUtc = timeProvider.GetUtcNow();
            _subscriptions =
            [
                activity.Local.Subscribe(Local),
                activity.Remote.Subscribe(Remote),
                activity.SyncStates.Subscribe(Sync),
                activity.OperationStates.Subscribe(Operations),
                activity.Faults.Subscribe(Faults),
            ];
        }

        /// <summary>Gets when telemetry subscriptions were created.</summary>
        internal DateTimeOffset CreatedAtUtc { get; }

        /// <summary>Gets the local view observer.</summary>
        internal RecordingObserver<ActivityView> Local { get; }

        /// <summary>Gets the remote message observer.</summary>
        internal RecordingObserver<RemoteMessage<ActivityUpdate>> Remote { get; }

        /// <summary>Gets the synchronization state observer.</summary>
        internal RecordingObserver<SyncState> Sync { get; }

        /// <summary>Gets the operation status observer.</summary>
        internal RecordingObserver<SyncOperationStatus> Operations { get; }

        /// <summary>Gets the stream fault observer.</summary>
        internal RecordingObserver<OccasionallyConnectedFault> Faults { get; }

        /// <summary>Gets a value indicating whether any telemetry stream ended with an observer error.</summary>
        internal bool HasTerminalError =>
            Local.HasTerminalError
            || Remote.HasTerminalError
            || Sync.HasTerminalError
            || Operations.HasTerminalError
            || Faults.HasTerminalError;

        /// <inheritdoc />
        public void Dispose()
        {
            for (var index = _subscriptions.Length - 1; index >= 0; index--)
            {
                _subscriptions[index].Dispose();
            }
        }
    }

    /// <summary>Records observer values and lets tests wait for observable causal signals.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Protects observer state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores observed values.</summary>
        private readonly List<T> _values = [];

        /// <summary>Stores pending waits.</summary>
        private readonly List<TaskCompletionSource<T>> _waiters = [];

        /// <summary>The maximum retained value count.</summary>
        private readonly int _capacity;

        /// <summary>Stores the terminal observer error.</summary>
        private Exception? _terminalError;

        /// <summary>Initializes a new instance of the <see cref="RecordingObserver{T}"/> class.</summary>
        /// <param name="capacity">The maximum retained value count.</param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is less than or equal to zero.</exception>
        internal RecordingObserver(int capacity)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

            _capacity = capacity;
        }

        /// <summary>Gets a value indicating whether the observer recorded a terminal error.</summary>
        internal bool HasTerminalError
        {
            get
            {
                lock (_gate)
                {
                    return _terminalError is not null;
                }
            }
        }

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                RecordTerminalErrorLocked(error);
            }
        }

        /// <inheritdoc />
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_terminalError is not null)
                {
                    return;
                }

                if (_values.Count >= _capacity)
                {
                    RecordTerminalErrorLocked(new InvalidOperationException("The recording observer exceeded its bounded capacity."));
                    return;
                }

                _values.Add(value);
                for (var index = 0; index < _waiters.Count; index++)
                {
                    _ = _waiters[index].TrySetResult(value);
                }

                _waiters.Clear();
            }
        }

        /// <summary>Counts all matching retained values.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <returns>The matching count.</returns>
        internal int Count(Func<T, bool> predicate)
        {
            lock (_gate)
            {
                ThrowTerminalErrorLocked();
                return _values.Count(predicate);
            }
        }

        /// <summary>Gets a retained value snapshot.</summary>
        /// <returns>The retained values.</returns>
        internal T[] Snapshot()
        {
            lock (_gate)
            {
                ThrowTerminalErrorLocked();
                return [.. _values];
            }
        }

        /// <summary>Waits for a matching observed value.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>The matching value.</returns>
        internal async Task<T> WaitForAsync(Func<T, bool> predicate, TimeSpan timeout)
        {
            using var cancellation = new CancellationTokenSource(timeout);
            while (true)
            {
                var (task, waiter) = GetWaitTask(predicate);
                if (waiter is null)
                {
                    return await task.ConfigureAwait(false);
                }

                T observed;
                try
                {
                    observed = await task.WaitAsync(cancellation.Token).ConfigureAwait(false);
                }
                finally
                {
                    RemoveWaiter(waiter);
                }

                if (predicate(observed))
                {
                    return observed;
                }
            }
        }

        /// <summary>Gets a completed matching value task or registers a waiter.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <returns>The wait task and optional waiter.</returns>
        private (Task<T> Task, TaskCompletionSource<T>? Waiter) GetWaitTask(Func<T, bool> predicate)
        {
            lock (_gate)
            {
                ThrowTerminalErrorLocked();
                for (var index = 0; index < _values.Count; index++)
                {
                    if (predicate(_values[index]))
                    {
                        return (Task.FromResult(_values[index]), null);
                    }
                }

                var waiter = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add(waiter);
                return (waiter.Task, waiter);
            }
        }

        /// <summary>Removes a waiter after cancellation or completion.</summary>
        /// <param name="waiter">The waiter to remove.</param>
        private void RemoveWaiter(TaskCompletionSource<T> waiter)
        {
            lock (_gate)
            {
                _ = _waiters.Remove(waiter);
            }
        }

        /// <summary>Records a terminal observer error while the gate is held.</summary>
        /// <param name="error">The terminal error.</param>
        private void RecordTerminalErrorLocked(Exception error)
        {
            _terminalError ??= error;
            for (var index = 0; index < _waiters.Count; index++)
            {
                _ = _waiters[index].TrySetException(_terminalError);
            }

            _waiters.Clear();
        }

        /// <summary>Throws the recorded terminal error while the gate is held.</summary>
        /// <exception cref="InvalidOperationException">The recording observer recorded a terminal error.</exception>
        private void ThrowTerminalErrorLocked()
        {
            if (_terminalError is not null)
            {
                throw new InvalidOperationException("The recording observer ended with a terminal error.", _terminalError);
            }
        }
    }

    /// <summary>Observes activity views and lets tests wait for public stream state.</summary>
    private sealed class ActivityViewObserver : IObserver<ActivityView>
    {
        /// <summary>Protects observer state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores pending waits.</summary>
        private readonly List<TaskCompletionSource<ActivityView>> _waiters = [];

        /// <summary>Stores the latest view.</summary>
        private ActivityView? _latest;

        /// <summary>Stores the terminal observer error.</summary>
        private Exception? _terminalError;

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                RecordTerminalErrorLocked(error);
            }
        }

        /// <inheritdoc />
        public void OnNext(ActivityView value)
        {
            lock (_gate)
            {
                if (_terminalError is not null)
                {
                    return;
                }

                _latest = value;
                for (var index = 0; index < _waiters.Count; index++)
                {
                    _ = _waiters[index].TrySetResult(value);
                }

                _waiters.Clear();
            }
        }

        /// <summary>Waits for a matching activity view.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>The matching view.</returns>
        internal async Task<ActivityView> WaitForAsync(Func<ActivityView, bool> predicate, TimeSpan timeout)
        {
            using var cancellation = new CancellationTokenSource(timeout);
            while (true)
            {
                var (task, waiter) = GetWaitTask(predicate);
                if (waiter is null)
                {
                    return await task.ConfigureAwait(false);
                }

                ActivityView observed;
                try
                {
                    observed = await task.WaitAsync(cancellation.Token).ConfigureAwait(false);
                }
                finally
                {
                    RemoveWaiter(waiter);
                }

                if (predicate(observed))
                {
                    return observed;
                }
            }
        }

        /// <summary>Gets a completed matching value task or registers a waiter.</summary>
        /// <param name="predicate">The predicate.</param>
        /// <returns>The wait task and optional waiter.</returns>
        private (Task<ActivityView> Task, TaskCompletionSource<ActivityView>? Waiter) GetWaitTask(Func<ActivityView, bool> predicate)
        {
            lock (_gate)
            {
                ThrowTerminalErrorLocked();
                if (_latest is { } latest && predicate(latest))
                {
                    return (Task.FromResult(latest), null);
                }

                var waiter = new TaskCompletionSource<ActivityView>(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add(waiter);
                return (waiter.Task, waiter);
            }
        }

        /// <summary>Removes a waiter after cancellation or completion.</summary>
        /// <param name="waiter">The waiter to remove.</param>
        private void RemoveWaiter(TaskCompletionSource<ActivityView> waiter)
        {
            lock (_gate)
            {
                _ = _waiters.Remove(waiter);
            }
        }

        /// <summary>Records a terminal observer error while the gate is held.</summary>
        /// <param name="error">The terminal error.</param>
        private void RecordTerminalErrorLocked(Exception error)
        {
            _terminalError ??= error;
            for (var index = 0; index < _waiters.Count; index++)
            {
                _ = _waiters[index].TrySetException(_terminalError);
            }

            _waiters.Clear();
        }

        /// <summary>Throws the recorded terminal error while the gate is held.</summary>
        /// <exception cref="InvalidOperationException">The activity observer recorded a terminal error.</exception>
        private void ThrowTerminalErrorLocked()
        {
            if (_terminalError is not null)
            {
                throw new InvalidOperationException("The activity observer ended with a terminal error.", _terminalError);
            }
        }
    }
}
