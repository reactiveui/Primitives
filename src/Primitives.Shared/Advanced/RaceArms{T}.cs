// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Shared race-arm bookkeeping: tracks candidate subscriptions, elects a winner, and disposes losers.</summary>
/// <typeparam name="T">The source value type.</typeparam>
internal sealed class RaceArms<T> : IDisposable
{
    /// <summary>Serializes source registration and winner finalization.</summary>
    private readonly Lock _gate = new();

    /// <summary>Active source subscriptions by source index.</summary>
    private readonly Dictionary<int, IDisposable> _sourceSubscriptions = [];

    /// <summary>The active subscriptions.</summary>
    private readonly MultipleDisposable _subscriptions = [];

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The winning source index, or -1 before a source wins.</summary>
    private int _winner = -1;

    /// <summary>The next source index.</summary>
    private int _index;

    /// <summary>Initializes a new instance of the <see cref="RaceArms{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    internal RaceArms(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public void Dispose()
    {
        _subscriptions.Dispose();
        lock (_gate)
        {
            _sourceSubscriptions.Clear();
        }
    }

    /// <summary>Adds a disposable that should be released with the arms.</summary>
    /// <param name="disposable">The disposable to track.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Add(IDisposable disposable) => _subscriptions.Add(disposable);

    /// <summary>Forwards a terminal error from the outer source to the downstream observer.</summary>
    /// <param name="error">The error to forward.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void OnOuterError(Exception error) => _observer.OnError(error);

    /// <summary>Subscribes to a candidate source and registers it for winner bookkeeping.</summary>
    /// <param name="source">The candidate source.</param>
    internal void OnSource(IObservable<T> source)
    {
        var current = Interlocked.Increment(ref _index) - 1;
        var subscription = source.Subscribe(
            value => OnNext(current, value),
            error => OnError(current, error),
            () => OnCompleted(current));
        _subscriptions.Add(subscription);

        lock (_gate)
        {
            if (_winner < 0 || _winner == current)
            {
                _sourceSubscriptions[current] = subscription;
                return;
            }

            _ = _subscriptions.Remove(subscription);
        }
    }

    /// <summary>Forwards a value from the winning candidate.</summary>
    /// <param name="candidate">The candidate index.</param>
    /// <param name="value">The value to forward.</param>
    private void OnNext(int candidate, T value)
    {
        if (!Win(candidate))
        {
            return;
        }

        _observer.OnNext(value);
    }

    /// <summary>Forwards an error from the winning candidate.</summary>
    /// <param name="candidate">The candidate index.</param>
    /// <param name="error">The error to forward.</param>
    private void OnError(int candidate, Exception error)
    {
        if (!Win(candidate))
        {
            return;
        }

        _observer.OnError(error);
    }

    /// <summary>Forwards completion from the winning candidate.</summary>
    /// <param name="candidate">The candidate index.</param>
    private void OnCompleted(int candidate)
    {
        if (!Win(candidate))
        {
            return;
        }

        _observer.OnCompleted();
    }

    /// <summary>Attempts to make a candidate the winner, disposing the losers when it wins.</summary>
    /// <param name="candidate">The candidate index.</param>
    /// <returns><see langword="true"/> when the candidate is the winner.</returns>
    private bool Win(int candidate)
    {
        var current = Volatile.Read(ref _winner);
        if (current == candidate)
        {
            return true;
        }

        if (current >= 0)
        {
            return false;
        }

        if (Interlocked.CompareExchange(ref _winner, candidate, -1) != -1)
        {
            return false;
        }

        DiscardLosers(candidate);
        return true;
    }

    /// <summary>Disposes all inner subscriptions except the winner.</summary>
    /// <param name="winner">The winning source index.</param>
    private void DiscardLosers(int winner)
    {
        List<(int Candidate, IDisposable Subscription)> losers = [];
        lock (_gate)
        {
            foreach (var pair in _sourceSubscriptions)
            {
                if (pair.Key != winner)
                {
                    losers.Add((pair.Key, pair.Value));
                }
            }

            for (var i = 0; i < losers.Count; i++)
            {
                _ = _sourceSubscriptions.Remove(losers[i].Candidate);
            }
        }

        for (var i = 0; i < losers.Count; i++)
        {
            _ = _subscriptions.Remove(losers[i].Subscription);
        }
    }
}
