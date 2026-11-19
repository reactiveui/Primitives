// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>SubscribeOn</c> (defer subscription to a sequencer).</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SubscribeOnSignal: Source = {_source}")]
public sealed class SubscribeOnSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The sequencer the subscription is scheduled onto.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="SubscribeOnSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="scheduler">The sequencer the subscription is scheduled onto.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public SubscribeOnSignal(IObservable<T> source, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _source = source;
        _scheduler = scheduler;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _scheduler.IsImmediate
            ? _source.Subscribe(observer)
            : _scheduler.Schedule(
                (Source: _source, Observer: observer),
                static (_, s) => s.Source.Subscribe(s.Observer));
    }
}
