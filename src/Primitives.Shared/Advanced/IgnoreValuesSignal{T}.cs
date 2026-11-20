// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that ignores source values and forwards terminal notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("IgnoreValuesSignal: Source = {Source}")]
public sealed class IgnoreValuesSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="IgnoreValuesSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public IgnoreValuesSignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Applies <c>IgnoreValues</c>/<c>IgnoreElements</c> reductions and idempotence.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The reduced observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source switch
        {
            ImmutableEmptySignal<T> or ImmutableNeverSignal<T> or IgnoreValuesSignal<T> => source,
            RangeSignal or ImmediateReturnSignal<T> => ImmutableEmptySignal<T>.Instance,
            _ => new IgnoreValuesSignal<T>(source),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        IgnoreValuesWitness<T> sink = new(observer);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
