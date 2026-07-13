// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer that collects source values into batches.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class CollectWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>Serializes access to buffered values and terminal state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Whether a flush has already been scheduled for the current window.</summary>
    private bool _flushScheduled;

    /// <summary>Whether the source has terminated.</summary>
    private bool _stopped;

    /// <summary>Initializes a new instance of the <see cref="CollectWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public CollectWitness(IObserver<IList<T>> observer) =>
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));

    /// <summary>Initializes a new instance of the <see cref="CollectWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="timeSpan">The buffer window duration.</param>
    /// <param name="sequencer">The sequencer used to schedule flushes.</param>
    public CollectWitness(IObserver<IList<T>> observer, TimeSpan timeSpan, ISequencer sequencer)
    {
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));
        TimeSpan = timeSpan;
        Sequencer = sequencer ?? throw new ArgumentNullException(nameof(sequencer));
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<IList<T>> Observer { get; }

    /// <summary>Gets the buffer window duration.</summary>
    private TimeSpan TimeSpan { get; }

    /// <summary>Gets the sequencer used to schedule flushes.</summary>
    private ISequencer? Sequencer { get; }

    /// <summary>Gets the source subscription and scheduled flushes.</summary>
    private MultipleDisposable Disposables { get; } = [];

    /// <summary>Gets the values collected for the current window.</summary>
    private List<T> Values { get; } = [];

    /// <inheritdoc/>
    public void Dispose()
    {
        _ = TryMarkStopped();
        Disposables.Dispose();
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (!TryCompleteAndTakeFinalBatch(out var batch))
        {
            return;
        }

        if (batch is { Length: > 0 })
        {
            Observer.OnNext(batch);
        }

        try
        {
            Observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (!TryMarkStopped())
        {
            return;
        }

        try
        {
            Observer.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Sequencer is null)
        {
            if (IsStopped())
            {
                return;
            }

            Observer.OnNext([value]);
            return;
        }

        if (!TryRecord(value))
        {
            return;
        }

        Disposables.Add(Sequencer.Schedule(TimeSpan, Flush));
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => Disposables.Add(subscription);

    /// <summary>Flushes the current window if it still has buffered values.</summary>
    private void Flush()
    {
        var batch = TakeScheduledBatch();
        if (batch is not { Length: > 0 })
        {
            return;
        }

        Observer.OnNext(batch);
    }

    /// <summary>Gets a value indicating whether this observer has stopped.</summary>
    /// <returns><see langword="true"/> when the observer has stopped.</returns>
    private bool IsStopped()
    {
        lock (_gate)
        {
            return _stopped;
        }
    }

    /// <summary>Stores a value and reports whether this value opened a new scheduled window.</summary>
    /// <param name="value">The source value.</param>
    /// <returns><see langword="true"/> when a flush should be scheduled.</returns>
    private bool TryRecord(T value)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return false;
            }

            Values.Add(value);
            if (_flushScheduled)
            {
                return false;
            }

            _flushScheduled = true;
            return true;
        }
    }

    /// <summary>Marks the observer as stopped if it has not already stopped.</summary>
    /// <returns><see langword="true"/> when this call stopped the observer.</returns>
    private bool TryMarkStopped()
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return false;
            }

            _stopped = true;
            return true;
        }
    }

    /// <summary>Returns and clears values from a scheduled flush.</summary>
    /// <returns>The values to emit, or <see langword="null"/> when there is no batch.</returns>
    private T[]? TakeScheduledBatch()
    {
        lock (_gate)
        {
            _flushScheduled = false;
            return Values.Count == 0 || _stopped ? null : TakeValues();
        }
    }

    /// <summary>Stops the observer and returns the final buffered values.</summary>
    /// <param name="batch">The final buffered values, or <see langword="null"/> when no values remain.</param>
    /// <returns><see langword="true"/> when this call stopped the observer.</returns>
    private bool TryCompleteAndTakeFinalBatch(out T[]? batch)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                batch = null;
                return false;
            }

            _stopped = true;
            batch = Values.Count == 0 ? null : TakeValues();
            return true;
        }
    }

    /// <summary>Copies and clears the buffered values.</summary>
    /// <returns>The copied buffered values.</returns>
    private T[] TakeValues()
    {
        var batch = Values.ToArray();
        Values.Clear();
        return batch;
    }
}
