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
public sealed class ChainSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from an outer observable.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    public ChainSignal(IObservable<IObservable<T>> sources) => Sources = sources;

    /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from enumerable sources.</summary>
    /// <param name="sources">The sources to concatenate.</param>
    public ChainSignal(IEnumerable<IObservable<T>> sources) => EnumerableSources = sources;

    /// <summary>Initializes a new instance of the <see cref="ChainSignal{T}"/> class from two fixed sources.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    public ChainSignal(IObservable<T> first, IObservable<T> second)
    {
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
