// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable advanced operations on asynchronous observables, such as reference
/// counting for connectable observables. These utilities are intended to be used with types that implement asynchronous
/// observer patterns.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Reference-counting operators for a connectable observable source sequence.</summary>
    /// <param name="source">The connectable observable sequence to ref count. Cannot be null.</param>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    extension<T>(ConnectableSignalAsync<T> source)
    {
        /// <summary>
        /// Returns an observable sequence that connects to the underlying connectable observable when the first observer
        /// subscribes, and disconnects when the last observer unsubscribes.
        /// </summary>
        /// <remarks>This operator is useful for sharing a single subscription to the underlying connectable
        /// observable among multiple subscribers. When the last observer unsubscribes, the connection to the source is
        /// automatically disposed.</remarks>
        /// <returns>An observable sequence that stays connected to the source as long as there is at least one subscription.</returns>
        public SignalAsync<T> RefCount() =>
            new RefCountSignal<T>(source);
    }

    /// <summary>
    /// Async observable that automatically connects to the underlying connectable source when the first
    /// observer subscribes and disconnects when the last observer unsubscribes.
    /// </summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The connectable observable to manage with reference counting.</param>
    internal sealed class RefCountSignal<T>(ConnectableSignalAsync<T> source) : SignalAsync<T>, IDisposable
    {
        /// <summary>The asynchronous gate used to serialize subscribe and dispose operations.</summary>
        private readonly AsyncSerialGate _gate = new();

        /// <summary>The current number of active subscribers.</summary>
        private int _refCount;

        /// <summary>The single-assignment disposable holding the connection to the underlying connectable source.</summary>
        private SingleAssignmentDisposableAsync? _connection;

        /// <summary>Indicates whether this instance has been disposed.</summary>
        private bool _disposedValue;

        /// <summary>Disposes the ref-count observable, releasing the connection gate and any active connection.</summary>
        public void Dispose() => Dispose(true);

        /// <summary>Releases managed resources if <paramref name="disposing"/> is true.</summary>
        /// <param name="disposing">True to release managed resources; false when called from a finalizer.</param>
        [SuppressMessage(
            "Major Bug",
            "S4462:Calls to async methods should not be blocking",
            Justification = "IDisposable.Dispose is intrinsically synchronous; this method must tear down the async connection on the sync dispose path.")]
        internal void Dispose(bool disposing)
        {
            if (_disposedValue)
            {
                return;
            }

            if (disposing)
            {
                _gate.Dispose();
                _connection?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }

            _disposedValue = true;
        }

        /// <summary>
        /// Subscribes the specified observer, incrementing the reference count and connecting to the source
        /// if this is the first subscriber.
        /// </summary>
        /// <param name="observer">The observer to receive elements from the connectable source.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>An async disposable that decrements the reference count on disposal.</returns>
        [DebuggerStepThrough]
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            using (await _gate.EnterAsync(cancellationToken).ConfigureAwait(false))
            {
                // incr refCount before Subscribe(completed source decrement refCxount in Subscribe)
                ++_refCount;
                var needConnect = _refCount == 1;
                RefCountWitness refCountWitness = new(this, observer);
                var subcription = await source.SubscribeAsync(refCountWitness, cancellationToken).ConfigureAwait(false);
                if (needConnect && !refCountWitness.HasDisposed)
                {
                    SingleAssignmentDisposableAsync connection = new();
                    _connection = connection;
                    await connection.SetDisposableAsync(await source.ConnectAsync(cancellationToken).ConfigureAwait(false)).ConfigureAwait(false);
                }

                return subcription;
            }
        }

        /// <summary>
        /// Witness wrapper that forwards all notifications and decrements the parent's reference count on disposal,
        /// disconnecting from the source when the count reaches zero.
        /// </summary>
        /// <param name="parent">The parent ref-count observable.</param>
        /// <param name="observer">The downstream witness to forward notifications to.</param>
        internal sealed class RefCountWitness(RefCountSignal<T> parent, IObserverAsync<T> observer)
            : WitnessAsync<T>
        {
            /// <summary>Forwards an element to the downstream witness.</summary>
            /// <param name="value">The element to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                observer.OnNextAsync(value, cancellationToken);

            /// <summary>Forwards a non-fatal error to the downstream witness.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <summary>Forwards completion to the downstream observer.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

            /// <summary>Decrements the parent's reference count and disconnects from the source when it reaches zero.</summary>
            /// <returns>A task representing the asynchronous disposal operation.</returns>
            [DebuggerStepThrough]
            protected override async ValueTask DisposeAsyncCore()
            {
                using (await parent._gate.EnterAsync().ConfigureAwait(false))
                {
                    if (--parent._refCount == 0)
                    {
                        var connection = parent._connection;
                        parent._connection = null;
                        if (connection is not null)
                        {
                            await connection.DisposeAsync().ConfigureAwait(false);
                        }
                    }
                }

                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }
}
