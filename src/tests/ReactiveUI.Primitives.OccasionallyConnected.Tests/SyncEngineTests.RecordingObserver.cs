// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Observer test doubles for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Records observer callbacks.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Protects asynchronous observer callbacks.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores observed values.</summary>
        private readonly List<T> _values = [];

        /// <summary>Signals a requested callback count.</summary>
        private TaskCompletionSource? _countWaiter;

        /// <summary>The callback count required by the current waiter.</summary>
        private int _expectedCount;

        /// <summary>The last terminal observer error.</summary>
        private Exception? _error;

        /// <summary>Gets a stable snapshot of observed values.</summary>
        public List<T> Values
        {
            get
            {
                lock (_gate)
                {
                    return [.. _values];
                }
            }
        }

        /// <summary>Gets the last terminal observer error.</summary>
        public Exception? Error
        {
            get
            {
                lock (_gate)
                {
                    return _error;
                }
            }
        }

        /// <summary>Waits for the requested number of delivered callbacks.</summary>
        /// <param name="count">The callback count to observe.</param>
        /// <param name="timeout">The maximum wait.</param>
        /// <returns>The callback wait task.</returns>
        public Task WaitForCountAsync(int count, TimeSpan timeout)
        {
            Task waitTask;
            lock (_gate)
            {
                if (_values.Count >= count)
                {
                    waitTask = Task.CompletedTask;
                }
                else
                {
                    _expectedCount = count;
                    _countWaiter ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
                    waitTask = _countWaiter.Task;
                }
            }

            return waitTask.WaitAsync(timeout);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error)
        {
            ArgumentNullException.ThrowIfNull(error);
            lock (_gate)
            {
                _error = error;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value)
        {
            TaskCompletionSource? waiter = null;
            lock (_gate)
            {
                _values.Add(value);
                if (_countWaiter is not null && _values.Count >= _expectedCount)
                {
                    waiter = _countWaiter;
                    _countWaiter = null;
                }
            }

            _ = waiter?.TrySetResult();
        }
    }
}
