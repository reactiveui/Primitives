// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that completes immediately without emitting any items.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that completes immediately without producing any values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "There are no parameters to infer from; the caller states the element type: SignalAsync.None<int>().")]
    public static IObservableAsync<T> None<T>() => EmptySignalAsync<T>.Instance;

    /// <summary>Creates an observable sequence that completes immediately without emitting any items.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that completes immediately without producing any values.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "There are no parameters to infer from; the caller states the element type: SignalAsync.Empty<int>().")]
    public static IObservableAsync<T> Empty<T>() => EmptySignalAsync<T>.Instance;

    /// <summary>Signals successful completion on subscribe and hands back <see cref="DisposableAsync.Empty"/>, from a cached instance per element type.</summary>
    /// <typeparam name="T">The element type of the empty sequence.</typeparam>
    internal sealed class EmptySignalAsync<T> : IObservableAsync<T>
    {
        /// <summary>The shared singleton instance for <typeparamref name="T"/>.</summary>
        internal static readonly EmptySignalAsync<T> Instance = new();

        /// <summary>Initializes a new instance of the <see cref="EmptySignalAsync{T}"/> class.</summary>
        private EmptySignalAsync()
        {
        }

        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            return DisposableAsync.Empty;
        }
    }
}
