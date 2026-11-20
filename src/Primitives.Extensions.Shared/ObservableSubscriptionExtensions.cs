// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Extensions.Reactive.Operators;

namespace ReactiveUI.Primitives.Extensions.Reactive;
#else
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions;
#endif

/// <summary>Provides extension methods that subscribe to a sequence and block the calling thread until it produces a value or terminates, returning the emitted value or the captured error.</summary>
public static class ObservableSubscriptionExtensions
{
    /// <summary>Blocking subscribe and wait operators for a <see cref="RxVoid"/>-producing source.</summary>
    /// <param name="source">The observable to subscribe to.</param>
    extension(IObservable<RxVoid> source)
    {
        /// <summary>Subscribes to a synchronously terminating sequence and discards its value.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SubscribeAndComplete() =>
            BlockingSubscription.SubscribeAndComplete(source);

        /// <summary>Subscribes to the source and returns any error emitted during the synchronous <see cref="IObservable{T}.Subscribe(IObserver{T})"/> call.</summary>
        /// <returns>The captured error, or <see langword="null"/> if none.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Exception? SubscribeGetError() =>
            BlockingSubscription.SubscribeGetError(source);

        /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes (default 30s timeout); rethrows any captured error.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitForCompletion() =>
            BlockingSubscription.WaitForCompletion(source);

        /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes, honoring an explicit <paramref name="timeout"/>; rethrows any captured error.</summary>
        /// <param name="timeout">The wait timeout.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitForCompletion(TimeSpan timeout) =>
            BlockingSubscription.WaitForCompletion(source, timeout);

        /// <summary>Blocks until the <see cref="RxVoid"/>-producing source completes, routing the subscribe call through <paramref name="scheduler"/>; rethrows any captured error.</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitForCompletion(ISequencer scheduler) =>
            BlockingSubscription.WaitForCompletion(source, scheduler);

        /// <summary>
        /// Blocks until the <see cref="RxVoid"/>-producing source completes, routing the subscribe call
        /// through <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>; rethrows any captured error.
        /// </summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <param name="timeout">The wait timeout.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WaitForCompletion(ISequencer scheduler, TimeSpan timeout) =>
            BlockingSubscription.WaitForCompletion(source, scheduler, timeout);
    }

    /// <summary>Blocking subscribe and wait operators for an observable source sequence.</summary>
    /// <typeparam name="T">The element type of the source.</typeparam>
    /// <param name="source">The observable to subscribe to.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Subscribes to the source and returns the last value emitted during the synchronous <see cref="IObservable{T}.Subscribe(IObserver{T})"/> call.</summary>
        /// <returns>The last emitted value, or <see langword="default"/> if no value was emitted.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T? SubscribeGetValue() =>
            BlockingSubscription.SubscribeGetValue(source);

        /// <summary>Subscribes to the source and returns any error emitted during the synchronous <see cref="IObservable{T}.Subscribe(IObserver{T})"/> call.</summary>
        /// <returns>The captured error, or <see langword="null"/> if none.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Exception? SubscribeGetError() =>
            BlockingSubscription.SubscribeGetError(source);

        /// <summary>Blocks until the source emits a value, errors, or completes (default 30s timeout).</summary>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate in time.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T? WaitForValue() =>
            BlockingSubscription.WaitForValue(source);

        /// <summary>Blocks until the source emits a value, errors, or completes, honoring an explicit <paramref name="timeout"/>.</summary>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T? WaitForValue(TimeSpan timeout) =>
            BlockingSubscription.WaitForValue(source, timeout);

        /// <summary>Blocks until the source emits a value, errors, or completes, routing the subscribe call through <paramref name="scheduler"/> (default 30s timeout).</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate in time.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T? WaitForValue(ISequencer scheduler) =>
            BlockingSubscription.WaitForValue(source, scheduler);

        /// <summary>Blocks until the source emits a value, errors, or completes, routing the subscribe call through <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>.</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The last value emitted before terminal, or <see langword="default"/> if the sequence completed empty.</returns>
        /// <exception cref="TimeoutException">The sequence did not terminate within <paramref name="timeout"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T? WaitForValue(ISequencer scheduler, TimeSpan timeout) =>
            BlockingSubscription.WaitForValue(source, scheduler, timeout);

        /// <summary>Blocks until the source terminates or the timeout expires, returning any captured error without throwing it.</summary>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Exception? WaitForError() =>
            BlockingSubscription.WaitForError(source);

        /// <summary>Blocks until the source terminates with an explicit <paramref name="timeout"/>; returns any captured error (does NOT rethrow).</summary>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Exception? WaitForError(TimeSpan timeout) =>
            BlockingSubscription.WaitForError(source, timeout);

        /// <summary>Blocks until the source terminates, routing the subscribe call through <paramref name="scheduler"/>; returns any captured error (does NOT rethrow).</summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Exception? WaitForError(ISequencer scheduler) =>
            BlockingSubscription.WaitForError(source, scheduler);

        /// <summary>
        /// Blocks until the source terminates, routing the subscribe call
        /// through <paramref name="scheduler"/> with an explicit <paramref name="timeout"/>;
        /// returns any captured error (does NOT rethrow).
        /// </summary>
        /// <param name="scheduler">Scheduler used to dispatch the subscribe call.</param>
        /// <param name="timeout">The wait timeout.</param>
        /// <returns>The captured error, or <see langword="null"/> if completion was normal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Exception? WaitForError(ISequencer scheduler, TimeSpan timeout) =>
            BlockingSubscription.WaitForError(source, scheduler, timeout);
    }
}
