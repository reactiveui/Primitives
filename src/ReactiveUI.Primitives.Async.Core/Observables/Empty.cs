// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides factory methods for creating asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static methods for constructing instances of asynchronous
/// observables. Use these methods to create observable sequences that support asynchronous notification
/// patterns.</remarks>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that completes immediately without emitting any items.</summary>
    /// <remarks>This method is useful for representing an empty sequence in asynchronous or reactive
    /// scenarios. The returned sequence signals completion to observers as soon as it is subscribed to.
    /// The returned instance is a process-wide singleton per element type — no allocation occurs after the
    /// first call for a given <typeparamref name="T"/>.</remarks>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that completes immediately without producing any values.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SignalAsync.Empty<int>().")]
    public static IObservableAsync<T> None<T>() => EmptySignalAsync<T>.Instance;

    /// <summary>Creates an observable sequence that completes immediately without emitting any items.</summary>
    /// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
    /// <returns>An observable sequence of type <typeparamref name="T"/> that completes immediately without producing any values.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S4018:Generic methods should provide type parameters",
        Justification = "Public factory API — caller specifies T explicitly: SignalAsync.Empty<int>().")]
    public static IObservableAsync<T> Empty<T>() => None<T>();

    /// <summary>
    /// Dedicated singleton observable that signals immediate successful completion on subscribe. Replaces the
    /// previous <c>Create&lt;T&gt;((observer, _) =&gt; ...)</c> + <see cref="DisposableAsync.Empty"/> shape with a
    /// per-T cached instance — no anonymous observable wrapper, no closure, no per-subscribe allocation.
    /// </summary>
    /// <typeparam name="T">The element type of the empty sequence.</typeparam>
    internal sealed class EmptySignalAsync<T> : IObservableAsync<T>
    {
        /// <summary>The shared singleton instance for <typeparamref name="T"/>.</summary>
        public static readonly EmptySignalAsync<T> Instance = new();

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
