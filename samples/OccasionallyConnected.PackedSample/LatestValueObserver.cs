// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.PackedSample;

/// <summary>Keeps the latest value of an observable and lets the sample wait, with a timeout, for a condition.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class LatestValueObserver<T> : IObserver<T>
{
    private readonly object _gate = new();
    private readonly List<(Func<T, bool> Condition, TaskCompletionSource<T> Completion)> _waiters = [];
    private T? _latest;
    private bool _hasValue;

    /// <summary>Gets the latest value, or the default when nothing arrived yet.</summary>
    internal T? Latest
    {
        get
        {
            lock (_gate)
            {
                return _latest;
            }
        }
    }

    /// <summary>Waits until the latest value satisfies a condition.</summary>
    /// <param name="condition">The condition.</param>
    /// <param name="timeout">The longest time to wait.</param>
    /// <returns>The matching value, or the latest value (or default) when the wait timed out, and whether it matched.</returns>
    internal async Task<(bool Matched, T? Value)> WaitAsync(Func<T, bool> condition, TimeSpan timeout)
    {
        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            if (_hasValue && condition(_latest!))
            {
                return (true, _latest);
            }

            _waiters.Add((condition, completion));
        }

        var finished = await Task.WhenAny(completion.Task, Task.Delay(timeout)).ConfigureAwait(false);
        if (finished == completion.Task)
        {
            return (true, await completion.Task.ConfigureAwait(false));
        }

        lock (_gate)
        {
            _ = _waiters.RemoveAll(waiter => waiter.Completion == completion);
            return (false, _latest);
        }
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        List<TaskCompletionSource<T>> matched = [];
        lock (_gate)
        {
            _latest = value;
            _hasValue = true;
            foreach (var waiter in _waiters)
            {
                if (waiter.Condition(value))
                {
                    matched.Add(waiter.Completion);
                }
            }
        }

        foreach (var completion in matched)
        {
            _ = completion.TrySetResult(value);
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
    }
}
