// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Async.Reactive.Advanced;

namespace ReactiveUI.Primitives.Async.Reactive;
#else
using ReactiveUI.Primitives.Async.Advanced;

namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides shim-typed parity helper operators that depend on <see cref="AsyncContext"/> and <see cref="RxVoid"/>.</summary>
[SuppressMessage(
    "StyleCop.CSharp.OrderingRules",
    "SA1201:ElementsShouldAppearInTheCorrectOrder",
    Justification = "Extension block members cannot be reordered into the conventional element order.")]
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Async-native parity helper operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Converts the source sequence into a signal sequence that emits <see cref="RxVoid.Default"/> for each source value.</summary>
        /// <returns>An observable sequence of <see cref="RxVoid"/> values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<RxVoid> AsSignal() =>
            new AsRxVoidSignal<T>(source);

        /// <summary>Observes the source on the supplied context, handing back the source unchanged when no context is supplied.</summary>
        /// <param name="asyncContext">The target async context, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnSafe(AsyncContext? asyncContext) =>
            WitnessOnSignal<T>.CreateSafe(source, asyncContext);

        /// <summary>Observes the source on the supplied context, handing back the source unchanged when no context is supplied.</summary>
        /// <param name="asyncContext">The target async context, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnSafe(AsyncContext? asyncContext, bool forceYielding) =>
            WitnessOnSignal<T>.CreateSafe(source, asyncContext, forceYielding);

        /// <summary>Observes the source on the supplied scheduler, handing back the source unchanged when no scheduler is supplied.</summary>
        /// <param name="taskScheduler">The target scheduler, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnSafe(TaskScheduler? taskScheduler) =>
            WitnessOnSignal<T>.CreateSafe(source, taskScheduler);

        /// <summary>Observes the source on the supplied scheduler, handing back the source unchanged when no scheduler is supplied.</summary>
        /// <param name="taskScheduler">The target scheduler, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnSafe(TaskScheduler? taskScheduler, bool forceYielding) =>
            WitnessOnSignal<T>.CreateSafe(source, taskScheduler, forceYielding);

        /// <summary>Observes the source on the provided context only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="asyncContext">The target async context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnIf(bool condition, AsyncContext asyncContext) =>
            WitnessOnSignal<T>.CreateIf(source, condition, asyncContext);

        /// <summary>Observes the source on the provided context only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="asyncContext">The target async context.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnIf(bool condition, AsyncContext asyncContext, bool forceYielding) =>
            WitnessOnSignal<T>.CreateIf(source, condition, asyncContext, forceYielding);

        /// <summary>Observes the source on the provided scheduler only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="taskScheduler">The target scheduler.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnIf(bool condition, TaskScheduler taskScheduler) =>
            WitnessOnSignal<T>.CreateIf(source, condition, taskScheduler);

        /// <summary>Observes the source on the provided scheduler only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="taskScheduler">The target scheduler.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> ObserveOnIf(bool condition, TaskScheduler taskScheduler, bool forceYielding) =>
            WitnessOnSignal<T>.CreateIf(source, condition, taskScheduler, forceYielding);
    }
}
