// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Witness state operations reachable through <see cref="IWitnessState"/> and <see cref="IWitnessAsync{T}"/>.</summary>
public static class WitnessAsyncExtensions
{
    /// <summary>Witness operations that reach the witness's hooks.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    extension<T>(IWitnessAsync<T> witness)
    {
        /// <summary>Disposes when the exiting call owns disposal, or signals an existing waiter.</summary>
        /// <returns>The disposal operation, or a completed task when a waiter owns disposal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask CompleteOrChainDispose() =>
            witness.Witness.ExitOnSomethingCall() ? witness.DisposeAsync() : default;

        /// <summary>Runs the error hook, reporting anything it throws, and the original error when the call is cancelled.</summary>
        /// <param name="error">The exception that triggered error handling.</param>
        /// <param name="cancellationToken">A cancellation token for the operation.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask RouteObserverErrorAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.RouteObserverErrorCoreAsync(witness, error, cancellationToken);
    }

    /// <summary>Witness state operations reachable through any <see cref="IWitnessState"/>.</summary>
    /// <param name="witness">The witness.</param>
    extension(IWitnessState witness)
    {
        /// <summary>Gets the lazily created token cancelled when the witness is disposed.</summary>
        public CancellationToken InternalDisposedToken
        {
            get
            {
                witness.Witness.EnsureLinked(witness);
                return witness.Witness.GetOrCreateDisposeCts().Token;
            }
        }

        /// <summary>Gets a value indicating whether the witness has been disposed.</summary>
        internal bool HasDisposed => witness.Witness.HasDisposed;

        /// <summary>Sets the source subscription disposable for the witness.</summary>
        /// <param name="value">The source subscription to track, or <see langword="null"/> to clear it.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AssignSourceSubscriptionAsync(IAsyncDisposable? value) =>
            witness.Witness.AssignSourceSubscriptionAsync(value);

        /// <summary>Makes an upstream witness's disposal dispose this witness too, so an operator chain tears down from the top.</summary>
        /// <param name="upstream">The upstream witness's dispose token.</param>
        public void LinkUpstreamCancellation(CancellationToken upstream)
        {
            witness.Witness.EnsureLinked(witness);
            witness.Witness.LinkExternalCancellation(witness, upstream);
        }

        /// <summary>Captures the owner thread and active notification count atomically.</summary>
        /// <returns>The packed notification state.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long ReadCallState() => witness.Witness.ReadCallState();

        /// <summary>Attempts to enter a notification call, checking for disposal, cancellation, and concurrent access.</summary>
        /// <param name="cancellationToken">The caller-supplied cancellation token.</param>
        /// <param name="scope">When successful, the scope providing the effective cancellation token.</param>
        /// <returns><see langword="true"/> if the call was entered successfully; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryEnterOnSomethingCall(CancellationToken cancellationToken, out LinkedTokenScope scope) =>
            witness.TryEnterOnSomethingCall(Environment.CurrentManagedThreadId, cancellationToken, out scope);

        /// <summary>Attempts to enter a notification for the supplied caller thread.</summary>
        /// <param name="currentThreadId">The calling thread identifier.</param>
        /// <param name="cancellationToken">Cancellation for the notification.</param>
        /// <param name="scope">The notification's effective cancellation scope.</param>
        /// <returns>Whether the caller acquired the notification gate.</returns>
        internal bool TryEnterOnSomethingCall(int currentThreadId, CancellationToken cancellationToken, out LinkedTokenScope scope)
        {
            witness.Witness.EnsureLinked(witness);
            return witness.Witness.TryEnterObservedCallState(currentThreadId, witness.Witness.ReadCallState(), cancellationToken, out scope);
        }

        /// <summary>Retries notification entry when the captured call state has changed.</summary>
        /// <param name="currentThreadId">The calling thread identifier.</param>
        /// <param name="oldState">The captured notification state.</param>
        /// <param name="cancellationToken">Cancellation for the notification.</param>
        /// <param name="scope">The notification's effective cancellation scope.</param>
        /// <returns>Whether entry succeeded.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryEnterObservedCallState(int currentThreadId, long oldState, CancellationToken cancellationToken, out LinkedTokenScope scope) =>
            witness.Witness.TryEnterObservedCallState(currentThreadId, oldState, cancellationToken, out scope);

        /// <summary>Leaves a notification call, releasing a disposal that is waiting on the last one.</summary>
        /// <returns><see langword="true"/> when the caller should proceed with disposal itself.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ExitOnSomethingCall() => witness.Witness.ExitOnSomethingCall();

        /// <summary>Retries notification exit when the captured call state has changed.</summary>
        /// <param name="oldState">The captured notification state.</param>
        /// <returns>Whether the exiting caller owns disposal instead of an existing waiter.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ExitObservedCallState(long oldState) => witness.Witness.ExitObservedCallState(oldState);

        /// <summary>Publishes the completion waiter and accounts for a notification that exited before publication.</summary>
        /// <returns>The task completed when the active call count reaches zero.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task PublishCallCompletionWaiter() => witness.Witness.PublishCallCompletionWaiter();

        /// <summary>Publishes a disposal source, releasing a losing allocation and preserving prior disposal.</summary>
        /// <returns>The published dispose source.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal CancellationTokenSource MaterializeDisposeCts() => witness.Witness.MaterializeDisposeCts();
    }
}
