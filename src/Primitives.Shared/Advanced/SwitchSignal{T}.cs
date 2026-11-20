// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mirrors only the latest inner observable from an outer source.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SwitchSignal: Sources = {Sources}")]
public sealed class SwitchSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="SwitchSignal{T}"/> class.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public SwitchSignal(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        Sources = sources;
    }

    /// <summary>Gets the outer source of inner observables.</summary>
    private IObservable<IObservable<T>> Sources { get; }

    /// <summary>Creates a switch signal with empty/never reductions and <c>Map(selector).Switch()</c> fusion.</summary>
    /// <param name="sources">The outer source of inner observables.</param>
    /// <returns>The switched observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<IObservable<T>> sources)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return sources switch
        {
            ImmutableEmptySignal<IObservable<T>> => ImmutableEmptySignal<T>.Instance,
            ImmutableNeverSignal<IObservable<T>> => ImmutableNeverSignal<T>.Instance,
            IMapFusable<IObservable<T>> map => map.FuseSwitch<T>(),
            _ => new SwitchSignal<T>(sources),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return SwitchRangeOptimizer.TrySubscribeRanges(Sources, observer)
            ? EmptyDisposable.Instance
            : new SwitchWitness<T>(observer).Run(Sources);
    }
}
