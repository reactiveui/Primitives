// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Merges inner observable sources from an outer observable concurrently.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("BlendSignal: Sources = {Sources}")]
public sealed class BlendSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="BlendSignal{T}"/> class.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public BlendSignal(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        Sources = sources;
    }

    /// <summary>Gets the outer source of inner observables.</summary>
    private IObservable<IObservable<T>> Sources { get; }

    /// <summary>Creates a concurrent inner-sequence merge signal with empty/never short-circuiting.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    /// <returns>The merged observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return sources switch
        {
            ImmutableEmptySignal<IObservable<T>> => ImmutableEmptySignal<T>.Instance,
            ImmutableNeverSignal<IObservable<T>> => ImmutableNeverSignal<T>.Instance,
            _ => new BlendSignal<T>(sources),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new BlendWitness<T>(observer).Run(Sources);
    }
}
