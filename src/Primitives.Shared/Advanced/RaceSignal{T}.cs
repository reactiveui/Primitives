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
    public RaceSignal(IObservable<IObservable<T>> sources) => Sources = sources;

    /// <summary>Initializes a new instance of the <see cref="RaceSignal{T}"/> class from enumerable sources.</summary>
    /// <param name="sources">The candidate sources.</param>
    public RaceSignal(IEnumerable<IObservable<T>> sources) => EnumerableSources = sources;

    /// <summary>Gets the outer source of candidate observables.</summary>
    private IObservable<IObservable<T>>? Sources { get; }

    /// <summary>Gets the enumerable candidate sources.</summary>
    private IEnumerable<IObservable<T>>? EnumerableSources { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        RaceWitness<T> coordinator = new(observer);
        return Sources is not null ? coordinator.Run(Sources) : coordinator.Run(EnumerableSources!);
    }
}
