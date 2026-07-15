// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer that emits only the latest value after a quiet period.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class EmitIfQuietWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>Serializes access to latest value and terminal state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The latest observed value.</summary>
    private T? _latest;

    /// <summary>Monotonic version used to suppress obsolete scheduled emissions.</summary>
    private long _version;

    /// <summary>Whether a latest value is pending emission.</summary>
    private bool _hasValue;

    /// <summary>Whether the source has terminated.</summary>
    private bool _stopped;

    /// <summary>Initializes a new instance of the <see cref="EmitIfQuietWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="dueTime">The quiet period before emitting the latest value.</param>
    /// <param name="sequencer">The sequencer used to schedule delayed emissions.</param>
    public EmitIfQuietWitness(IObserver<T> observer, TimeSpan dueTime, ISequencer sequencer)
    {
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));
        DueTime = dueTime;
        Sequencer = sequencer ?? throw new ArgumentNullException(nameof(sequencer));
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets the quiet period before emitting the latest value.</summary>
    private TimeSpan DueTime { get; }

    /// <summary>Gets the sequencer used to schedule delayed emissions.</summary>
    private ISequencer Sequencer { get; }

    /// <summary>Gets the source subscription and scheduled delayed emissions.</summary>
    private MultipleDisposable Disposables { get; } = [];

    /// <inheritdoc/>
    public void Dispose()
    {
        _ = TryMarkStopped();
        Disposables.Dispose();
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => Disposables.Add(subscription);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (!TryRecord(value, out var currentVersion))
        {
            return;
        }

        Disposables.Add(Sequencer.Schedule(
            (self: this, currentVersion),
            DueTime,
            static (_, s) =>
            {
                s.self.EmitIfLatest(s.currentVersion);
                return EmptyDisposable.Instance;
            }));
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
    public void OnCompleted()
    {
        if (!TryCompleteAndTakeLatest(out var value, out var hasValue))
        {
            return;
        }

        if (hasValue)
        {
            Observer.OnNext(value!);
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

    /// <summary>Emits the latest value if the scheduled version is still current.</summary>
    /// <param name="scheduledVersion">The version captured when the emission was scheduled.</param>
    private void EmitIfLatest(long scheduledVersion)
    {
        if (!TryTakeLatest(scheduledVersion, out var value))
        {
            return;
        }

        Observer.OnNext(value!);
    }

    /// <summary>Records a latest value and returns its version.</summary>
    /// <param name="value">The source value.</param>
    /// <param name="currentVersion">The version assigned to the value.</param>
    /// <returns><see langword="true"/> when delayed emission should be scheduled.</returns>
    private bool TryRecord(T value, out long currentVersion)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                currentVersion = default;
                return false;
            }

            _latest = value;
            _hasValue = true;
            currentVersion = ++_version;
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

    /// <summary>Returns and clears the pending value when the scheduled version is current.</summary>
    /// <param name="scheduledVersion">The version captured when the emission was scheduled.</param>
    /// <param name="value">The value to emit.</param>
    /// <returns><see langword="true"/> when a value should be emitted.</returns>
    private bool TryTakeLatest(long scheduledVersion, out T? value)
    {
        lock (_gate)
        {
            if (_stopped || !_hasValue || scheduledVersion != _version)
            {
                value = default;
                return false;
            }

            value = _latest;
            _hasValue = false;
            return true;
        }
    }

    /// <summary>Stops the observer and returns any pending latest value.</summary>
    /// <param name="value">The pending value to emit.</param>
    /// <param name="hasValue"><see langword="true"/> when a value should be emitted.</param>
    /// <returns><see langword="true"/> when this call stopped the observer.</returns>
    private bool TryCompleteAndTakeLatest(out T? value, out bool hasValue)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                value = default;
                hasValue = false;
                return false;
            }

            _stopped = true;
            if (!_hasValue)
            {
                value = default;
                hasValue = false;
                return true;
            }

            value = _latest;
            _hasValue = false;
            hasValue = true;
            return true;
        }
    }
}
