// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mirrors the first source that produces any notification.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("RaceSignal: Sources = {Sources}, EnumerableSources = {EnumerableSources}")]
public sealed class RaceSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="RaceSignal{T}"/> class from an outer observable.</summary>
    /// <param name="sources">The outer source of candidate observables.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public RaceSignal(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        Sources = sources;
    }

    /// <summary>Initializes a new instance of the <see cref="RaceSignal{T}"/> class from enumerable sources.</summary>
    /// <param name="sources">The candidate sources.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public RaceSignal(IEnumerable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        EnumerableSources = sources;
    }

    /// <summary>Gets the outer source of candidate observables.</summary>
    private IObservable<IObservable<T>>? Sources { get; }

    /// <summary>Gets the enumerable candidate sources.</summary>
    private IEnumerable<IObservable<T>>? EnumerableSources { get; }

    /// <summary>Creates a race signal from <paramref name="sources"/> with empty/never short-circuiting.</summary>
    /// <param name="sources">The outer source of candidate observables.</param>
    /// <returns>The racing observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return sources switch
        {
            ImmutableEmptySignal<IObservable<T>> => ImmutableEmptySignal<T>.Instance,
            ImmutableNeverSignal<IObservable<T>> => ImmutableNeverSignal<T>.Instance,
            _ => new RaceSignal<T>(sources),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        RaceWitness<T> coordinator = new(observer);
        return Sources is not null ? coordinator.Run(Sources) : coordinator.Run(EnumerableSources!);
    }
}
