// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates candidate subscriptions for <see cref="RaceSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class RaceWitness<T> : IDisposable
{
    /// <summary>The winning source index, or -1 before a source wins.</summary>
    private int _winner = -1;

    /// <summary>The next source index.</summary>
    private int _index;

    /// <summary>Initializes a new instance of the <see cref="RaceWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public RaceWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <inheritdoc/>
    public void Dispose() => Subscriptions.Dispose();

    /// <summary>Starts observing an outer observable of candidates.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public RaceWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        Subscriptions.Add(sources.Subscribe(OnSource, Observer.OnError, OnOuterCompleted));
        return this;
    }

    /// <summary>Starts observing enumerable candidates.</summary>
    /// <param name="sources">The candidate sources.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public RaceWitness<T> Run(IEnumerable<IObservable<T>> sources)
    {
        foreach (var source in sources)
        {
            OnSource(source);
        }

        OnOuterCompleted();
        return this;
    }

    /// <summary>Handles completion of the outer source.</summary>
    private static void OnOuterCompleted()
    {
        // Race completion is controlled by the first inner source to win.
    }

    /// <summary>Subscribes to a candidate source.</summary>
    /// <param name="source">The candidate source.</param>
    private void OnSource(IObservable<T> source)
    {
        if (source is null)
        {
            Observer.OnError(new InvalidOperationException("Race source contained null."));
            return;
        }

        var current = Interlocked.Increment(ref _index) - 1;
        Subscriptions.Add(source.Subscribe(
            value => OnNext(current, value),
            error => OnError(current, error),
            () => OnCompleted(current)));
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

        Observer.OnNext(value);
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

        Observer.OnError(error);
    }

    /// <summary>Forwards completion from the winning candidate.</summary>
    /// <param name="candidate">The candidate index.</param>
    private void OnCompleted(int candidate)
    {
        if (!Win(candidate))
        {
            return;
        }

        Observer.OnCompleted();
    }

    /// <summary>Attempts to make a candidate the winner.</summary>
    /// <param name="candidate">The candidate index.</param>
    /// <returns><see langword="true"/> when the candidate is the winner.</returns>
    private bool Win(int candidate)
    {
        var current = Volatile.Read(ref _winner);
        if (current == candidate)
        {
            return true;
        }

        return current >= 0 ? false : Interlocked.CompareExchange(ref _winner, candidate, -1) == -1;
    }
}
