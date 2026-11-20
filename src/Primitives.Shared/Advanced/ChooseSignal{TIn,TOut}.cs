// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>A fused filter + map observable.</summary>
/// <typeparam name="TIn">The source element type.</typeparam>
/// <typeparam name="TOut">The forwarded element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ChooseSignal<{typeof(TIn).Name,nq}>")]
public sealed class ChooseSignal<TIn, TOut> : IObservable<TOut>
{
    /// <summary>The source observable whose values are filtered and mapped.</summary>
    private readonly IObservable<TIn> _source;

    /// <summary>Maps a source value to <c>(HasValue, Value)</c>; skipped when <c>HasValue</c> is <see langword="false"/>.</summary>
    private readonly Func<TIn, (bool HasValue, TOut Value)> _chooser;

    /// <summary>Initializes a new instance of the <see cref="ChooseSignal{TIn, TOut}"/> class.</summary>
    /// <param name="source">The source observable whose values are filtered and mapped.</param>
    /// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="chooser"/> is <see langword="null"/>.</exception>
    public ChooseSignal(
        IObservable<TIn> source,
        Func<TIn, (bool HasValue, TOut Value)> chooser)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(chooser);

        _source = source;
        _chooser = chooser;
    }

    /// <summary>Creates a fused filter-and-map sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable whose values are filtered and mapped.</param>
    /// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
    /// <returns>An observable of the chosen values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="chooser"/> is <see langword="null"/>.</exception>
    public static IObservable<TOut> Create(
        IObservable<TIn> source,
        Func<TIn, (bool HasValue, TOut Value)> chooser)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(chooser);

        return source switch
        {
            ImmutableEmptySignal<TIn> => ImmutableEmptySignal<TOut>.Instance,
            ImmutableNeverSignal<TIn> => ImmutableNeverSignal<TOut>.Instance,
            _ => new ChooseSignal<TIn, TOut>(source, chooser),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TOut> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ChooseWitness<TIn, TOut> sink = new(observer, _chooser);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
