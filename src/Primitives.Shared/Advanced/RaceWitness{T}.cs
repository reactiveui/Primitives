// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates candidate subscriptions for <see cref="RaceSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Arms = {_arms}")]
public sealed class RaceWitness<T> : IDisposable
{
    /// <summary>The shared race-arm bookkeeping.</summary>
    private readonly RaceArms<T> _arms;

    /// <summary>Initializes a new instance of the <see cref="RaceWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public RaceWitness(IObserver<T> observer) => _arms = new(observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _arms.Dispose();

    /// <summary>Starts observing an outer observable of candidates.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public RaceWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        _arms.Add(sources.Subscribe(OnSource, _arms.OnOuterError, OnOuterCompleted));
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
            _arms.OnOuterError(new InvalidOperationException("Race source contained null."));
            return;
        }

        _arms.OnSource(source);
    }
}
