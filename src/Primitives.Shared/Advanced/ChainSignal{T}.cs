// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Concatenates observable sources in source order.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay(
"ChainSignal: Sources = {Sources}, EnumerableSources = {EnumerableSources}, First = {First}, Second = {Second}")]
public sealed class ChainSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from an outer observable.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public ChainSignal(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        Sources = sources;
    }

    /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from enumerable sources.</summary>
    /// <param name="sources">The sources to concatenate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public ChainSignal(IEnumerable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        EnumerableSources = sources;
    }

    /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from two fixed sources.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <exception cref="ArgumentNullException"><paramref name="first"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
    public ChainSignal(IObservable<T> first, IObservable<T> second)
    {
        ArgumentExceptionHelper.ThrowIfNull(first);
        ArgumentExceptionHelper.ThrowIfNull(second);

        First = first;
        Second = second;
    }

    /// <summary>Gets the outer source of inner observables.</summary>
    private IObservable<IObservable<T>>? Sources { get; }

    /// <summary>Gets the enumerable sources.</summary>
    private IEnumerable<IObservable<T>>? EnumerableSources { get; }

    /// <summary>Gets the first fixed source.</summary>
    private IObservable<T>? First { get; }

    /// <summary>Gets the second fixed source.</summary>
    private IObservable<T>? Second { get; }

    /// <summary>Creates a concatenated sequence from <paramref name="sources"/> with empty/never short-circuiting.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    /// <returns>The concatenated observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return sources switch
        {
            ImmutableEmptySignal<IObservable<T>> => ImmutableEmptySignal<T>.Instance,
            ImmutableNeverSignal<IObservable<T>> => ImmutableNeverSignal<T>.Instance,
            _ => new ChainSignal<T>(sources),
        };
    }

    /// <summary>Concatenates two sequences with empty/never identity reductions.</summary>
    /// <param name="first">The first sequence.</param>
    /// <param name="second">The second sequence.</param>
    /// <returns>The concatenated observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="first"/> or <paramref name="second"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> first, IObservable<T> second)
    {
        ArgumentExceptionHelper.ThrowIfNull(first);
        ArgumentExceptionHelper.ThrowIfNull(second);

        if (first is ImmutableEmptySignal<T>)
        {
            return second;
        }

        return second is ImmutableEmptySignal<T> || first is ImmutableNeverSignal<T>
            ? first
            : new ChainSignal<T>(first, second);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ChainWitness<T> coordinator = new(observer);
        if (Sources is not null)
        {
            return coordinator.Run(Sources);
        }

        return EnumerableSources is not null
            ? coordinator.Run(EnumerableSources)
            : coordinator.Run(First!, Second!);
    }
}
