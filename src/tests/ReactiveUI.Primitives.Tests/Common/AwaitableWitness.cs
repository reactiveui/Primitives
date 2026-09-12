// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Records callbacks and completes a task for each notification.</summary>
/// <typeparam name="T">The type of the observed values.</typeparam>
internal sealed class AwaitableWitness<T> : IObserver<T>
{
    /// <summary>The recorded values, in arrival order.</summary>
    private readonly ConcurrentQueue<T> _values = new();

    /// <summary>The recorded errors, in arrival order.</summary>
    private readonly ConcurrentQueue<Exception> _errors = new();

    /// <summary>The value-count thresholds a caller is waiting on, keyed by threshold.</summary>
    private readonly ConcurrentDictionary<int, TaskCompletionSource> _valueWaiters = new();

    /// <summary>Produces the first observed error.</summary>
    private readonly TaskCompletionSource<Exception> _firstError =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes when the sequence completes.</summary>
    private readonly TaskCompletionSource _completion =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Gets a task that produces the first observed error.</summary>
    internal Task<Exception> FirstError => _firstError.Task;

    /// <summary>Gets a task that completes when the sequence completes.</summary>
    internal Task Completion => _completion.Task;

    /// <summary>Gets a snapshot of the recorded values.</summary>
    internal IReadOnlyList<T> Values => [.. _values];

    /// <summary>Gets a snapshot of the recorded errors.</summary>
    internal IReadOnlyList<Exception> Errors => [.. _errors];

    /// <summary>Gets the number of completion callbacks observed.</summary>
    internal int Completions { get; private set; }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        Completions++;
        _ = _completion.TrySetResult();
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        _errors.Enqueue(error);
        _ = _firstError.TrySetResult(error);
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        _values.Enqueue(value);
        var count = _values.Count;
        foreach (var waiter in _valueWaiters)
        {
            if (waiter.Key <= count)
            {
                _ = waiter.Value.TrySetResult();
            }
        }
    }

    /// <summary>Gets a task that completes once the observed value count reaches a threshold.</summary>
    /// <param name="count">The value count to wait for.</param>
    /// <returns>A task that completes when at least <paramref name="count"/> values have been observed.</returns>
    internal Task ValueCountReaching(int count)
    {
        var waiter = _valueWaiters.GetOrAdd(
            count,
            static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        // Re-check after registering: a value that arrived in between would otherwise never signal this waiter.
        if (_values.Count >= count)
        {
            _ = waiter.TrySetResult();
        }

        return waiter.Task;
    }
}
