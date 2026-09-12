// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides static methods for creating and composing asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that never produces any values and never completes.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that never emits any items and never terminates.</returns>
    /// <remarks>The returned instance is a singleton per element type.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Design",
        "SST2307:Generic method type parameters should be inferable from the parameters",
        Justification = "There are no parameters to infer from; the caller states the element type: SignalAsync.Never<int>().")]
    public static IObservableAsync<T> Never<T>() => NeverSignalAsync<T>.Instance;

    /// <summary>An observable sequence that never produces any values and never completes.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    internal sealed class NeverSignalAsync<T> : IObservableAsync<T>
    {
        /// <summary>Gets the singleton instance of <see cref="NeverSignalAsync{T}"/>.</summary>
        internal static NeverSignalAsync<T> Instance { get; } = new();

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            new(DisposableAsync.Empty);
    }
}
