// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>Replaces each source element with <see cref="RxVoid.Default"/>, leaving error and completion untouched.</summary>
/// <typeparam name="T">The element type of the source observable (ignored).</typeparam>
/// <param name="source">The source observable whose values are ignored.</param>
public sealed class AsSignalObservable<T>(IObservable<T> source) : IObservable<RxVoid>
{
    /// <summary>Creates an <see cref="RxVoid"/> signal over <paramref name="source"/>, short-circuiting empty and never singletons.</summary>
    /// <param name="source">The source observable whose values are ignored.</param>
    /// <returns>An observable sequence emitting <see cref="RxVoid.Default"/> for each source value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<RxVoid> Create(IObservable<T> source) =>
        source switch
        {
            ImmutableEmptySignal<T> => ImmutableEmptySignal<RxVoid>.Instance,
            ImmutableNeverSignal<T> => ImmutableNeverSignal<RxVoid>.Instance,
            _ => new AsSignalObservable<T>(source),
        };

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new AsSignalWitness(observer));
    }

    /// <summary>Forwards every source value as <see cref="RxVoid.Default"/>, passing error and completion through unchanged.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class AsSignalWitness(IObserver<RxVoid> downstream) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => downstream.OnNext(RxVoid.Default);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
