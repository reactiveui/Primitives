// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Cold signal that splits a source into windows of a fixed number of values.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SliceCountSignal: Count = {_count}, Skip = {_skip}, Source = {_source}")]
public sealed class SliceCountSignal<T> : IObservable<IObservable<T>>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The number of values in each window.</summary>
    private readonly int _count;

    /// <summary>The number of values between the starts of consecutive windows.</summary>
    private readonly int _skip;

    /// <summary>Initializes a new instance of the <see cref="SliceCountSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The number of values in each window.</param>
    /// <param name="skip">The number of values between the starts of consecutive windows.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> or <paramref name="skip"/> is zero or negative.</exception>
    public SliceCountSignal(IObservable<T> source, int count, int skip)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(count);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(skip);
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _count = count;
        _skip = skip;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<IObservable<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SliceCountWitness<T> sink = new(observer, _count, _skip);
        sink.Start();
        sink.SetSubscription(_source.Subscribe(sink));
        return sink.Subscription;
    }
}
