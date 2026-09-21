// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Cold signal that opens a window each time an opening signal emits and ends it with a closing signal.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TOpening">The value type of the opening signal.</typeparam>
/// <typeparam name="TClosing">The value type of the closing signals.</typeparam>
[System.Diagnostics.DebuggerDisplay("SliceOpeningSignal: Source = {_source}")]
public sealed class SliceOpeningSignal<T, TOpening, TClosing> : IObservable<IObservable<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The signal whose emissions open a window.</summary>
    private readonly IObservable<TOpening> _openings;

    /// <summary>The selector that supplies the signal that ends the window an opening started.</summary>
    private readonly Func<TOpening, IObservable<TClosing>> _closingSelector;

    /// <summary>Initializes a new instance of the <see cref="SliceOpeningSignal{T, TOpening, TClosing}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="openings">The signal whose emissions open a window.</param>
    /// <param name="closingSelector">The selector that supplies the signal that ends the window an opening started.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public SliceOpeningSignal(
        IObservable<T> source,
        IObservable<TOpening> openings,
        Func<TOpening, IObservable<TClosing>> closingSelector)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _openings = openings ?? throw new ArgumentNullException(nameof(openings));
        _closingSelector = closingSelector ?? throw new ArgumentNullException(nameof(closingSelector));
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IObservable<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SliceOpeningWitness<T, TOpening, TClosing> sink = new(observer, _closingSelector);
        sink.SetSubscription(_source.Subscribe(sink));
        sink.SetSubscription(_openings.Subscribe(sink.Openings));
        return sink.Subscription;
    }
}
