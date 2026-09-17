// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>Subscribes indexed callbacks to each source and aggregates the subscription handles.</summary>
internal static class IndexedSubscribeHelper
{
    /// <summary>Subscribes callbacks carrying each source's index and returns a handle that disposes all subscriptions.</summary>
    /// <typeparam name="T">The element type of the source observables.</typeparam>
    /// <param name="sources">The source observables, indexed 0..N-1.</param>
    /// <param name="onNext">Per-source OnNext hook: <c>(index, value)</c>.</param>
    /// <param name="onError">Shared OnError hook (errors from any source forward unchanged).</param>
    /// <param name="onCompleted">Per-source OnCompleted hook: <c>(index)</c>.</param>
    /// <returns>A composite disposable that releases every per-source subscription on dispose.</returns>
    internal static IDisposable SubscribeIndexed<T>(
        IReadOnlyList<IObservable<T>> sources,
        Action<int, T> onNext,
        Action<Exception> onError,
        Action<int> onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);
        ArgumentExceptionHelper.ThrowIfNull(onNext);
        ArgumentExceptionHelper.ThrowIfNull(onError);
        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        DisposableBag composite = new();
        for (var i = 0; i < sources.Count; i++)
        {
            composite.Add(sources[i].Subscribe(new IndexedWitness<T>(i, onNext, onError, onCompleted)));
        }

        return composite;
    }

    /// <summary>Observer that tags each notification with its source index before invoking the shared hooks.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="index">The source index.</param>
    /// <param name="onNext">Per-source OnNext hook.</param>
    /// <param name="onError">Shared OnError hook.</param>
    /// <param name="onCompleted">Per-source OnCompleted hook.</param>
    private sealed class IndexedWitness<T>(
        int index,
        Action<int, T> onNext,
        Action<Exception> onError,
        Action<int> onCompleted) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => onNext(index, value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => onError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => onCompleted(index);
    }
}
