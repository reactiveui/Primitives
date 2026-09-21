// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Cold signal that splits a source into windows delimited by a boundary signal.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TBoundary">The value type of the boundary signal.</typeparam>
[System.Diagnostics.DebuggerDisplay("SliceBoundarySignal: Source = {_source}")]
public sealed class SliceBoundarySignal<T, TBoundary> : IObservable<IObservable<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The signal whose emissions start the next window.</summary>
    private readonly IObservable<TBoundary> _boundaries;

    /// <summary>Initializes a new instance of the <see cref="SliceBoundarySignal{T, TBoundary}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="boundaries">The signal whose emissions start the next window.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="boundaries"/> is <see langword="null"/>.</exception>
    public SliceBoundarySignal(IObservable<T> source, IObservable<TBoundary> boundaries)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _boundaries = boundaries ?? throw new ArgumentNullException(nameof(boundaries));
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IObservable<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SliceBoundaryWitness<T, TBoundary> sink = new(observer);
        sink.Start();
        sink.SetSubscription(_source.Subscribe(sink));
        sink.SetSubscription(_boundaries.Subscribe(sink.Boundaries));
        return sink.Subscription;
    }
}
