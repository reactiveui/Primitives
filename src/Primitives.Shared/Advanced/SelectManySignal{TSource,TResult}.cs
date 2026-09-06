// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Concrete signal for concurrent observable <c>SelectMany</c>.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SelectManySignal: Source = {Source}, Selector = {Selector}, Inner = {Inner}")]
public sealed class SelectManySignal<TSource, TResult> : IObservable<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="SelectManySignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="selector">The selector that creates an inner observable for each source value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public SelectManySignal(IObservable<TSource> source, Func<TSource, IObservable<TResult>> selector)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        Selector = selector ?? throw new ArgumentNullException(nameof(selector));
    }

    /// <summary>Initializes a new instance of the <see cref="SelectManySignal{TSource, TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="inner">The inner observable used for each source value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="inner"/> is <see langword="null"/>.</exception>
    public SelectManySignal(IObservable<TSource> source, IObservable<TResult> inner)
    {
        Source = source ?? throw new ArgumentNullException(nameof(source));
        Inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<TSource> Source { get; }

    /// <summary>Gets the selector, when this is selector-based.</summary>
    private Func<TSource, IObservable<TResult>>? Selector { get; }

    /// <summary>Gets the constant inner observable, when this is constant-inner based.</summary>
    private IObservable<TResult>? Inner { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return Selector is { } selector
            ? new SelectManyCoordinator<TSource, TResult>(observer, selector).Run(Source)
            : new SelectManyCoordinator<TSource, TResult>(observer, Inner!).Run(Source);
    }
}
