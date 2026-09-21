// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Cold signal that splits a source into consecutive windows, each ended by its own closing signal.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TClosing">The value type of the closing signals.</typeparam>
[System.Diagnostics.DebuggerDisplay("SliceClosingSignal: Source = {_source}")]
public sealed class SliceClosingSignal<T, TClosing> : IObservable<IObservable<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The selector that supplies the signal that ends a window.</summary>
    private readonly Func<IObservable<TClosing>> _closingSelector;

    /// <summary>Initializes a new instance of the <see cref="SliceClosingSignal{T, TClosing}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="closingSelector">The selector that supplies the signal that ends a window.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="closingSelector"/> is <see langword="null"/>.</exception>
    public SliceClosingSignal(IObservable<T> source, Func<IObservable<TClosing>> closingSelector)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _closingSelector = closingSelector ?? throw new ArgumentNullException(nameof(closingSelector));
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IObservable<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SliceClosingWitness<T, TClosing> sink = new(observer, _closingSelector);
        sink.Start();
        sink.SetSubscription(_source.Subscribe(sink));
        return sink.Subscription;
    }
}
