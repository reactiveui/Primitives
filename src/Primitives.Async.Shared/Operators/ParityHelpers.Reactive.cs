// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Async.Reactive.Advanced;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides shim-typed parity helper operators that depend on <see cref="AsyncContext"/> and <see cref="RxVoid"/>.</summary>
[SuppressMessage(
    "StyleCop.CSharp.OrderingRules",
    "SA1201:ElementsShouldAppearInTheCorrectOrder",
    Justification = "C# 14 extension methods")]
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Async-native parity helper operators for an observable source sequence.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Converts the source sequence into a signal sequence that emits <see cref="RxVoid.Default"/> for each source value.</summary>
        /// <returns>An observable sequence of <see cref="RxVoid"/> values.</returns>
        public IObservableAsync<RxVoid> AsSignal()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AsRxVoidSignal<T>(source);
        }

        /// <summary>Uses ObserveOn only when a context is provided.</summary>
        /// <param name="asyncContext">The target async context, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnSafe(AsyncContext? asyncContext)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return asyncContext is null ? source : new WitnessOnSignal<T>(source, asyncContext, false);
        }

        /// <summary>Uses ObserveOn only when a context is provided.</summary>
        /// <param name="asyncContext">The target async context, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnSafe(AsyncContext? asyncContext, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return asyncContext is null ? source : new WitnessOnSignal<T>(source, asyncContext, forceYielding);
        }

        /// <summary>Uses ObserveOn only when a scheduler is provided.</summary>
        /// <param name="taskScheduler">The target scheduler, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnSafe(TaskScheduler? taskScheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return taskScheduler is null
                ? source
                : new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), false);
        }

        /// <summary>Uses ObserveOn only when a scheduler is provided.</summary>
        /// <param name="taskScheduler">The target scheduler, or <see langword="null"/> to leave the sequence unchanged.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnSafe(TaskScheduler? taskScheduler, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return taskScheduler is null
                ? source
                : new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), forceYielding);
        }

        /// <summary>Observes the source on the provided context only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="asyncContext">The target async context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, AsyncContext asyncContext)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(asyncContext);

            return condition ? new WitnessOnSignal<T>(source, asyncContext, false) : source;
        }

        /// <summary>Observes the source on the provided context only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="asyncContext">The target async context.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided context.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, AsyncContext asyncContext, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(asyncContext);

            return condition ? new WitnessOnSignal<T>(source, asyncContext, forceYielding) : source;
        }

        /// <summary>Observes the source on the provided scheduler only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="taskScheduler">The target scheduler.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, TaskScheduler taskScheduler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(taskScheduler);

            return condition ? new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), false) : source;
        }

        /// <summary>Observes the source on the provided scheduler only when the condition is true.</summary>
        /// <param name="condition">A value indicating whether to switch context.</param>
        /// <param name="taskScheduler">The target scheduler.</param>
        /// <param name="forceYielding">Whether to force yielding when switching context.</param>
        /// <returns>The source sequence, optionally observed on the provided scheduler.</returns>
        public IObservableAsync<T> ObserveOnIf(bool condition, TaskScheduler taskScheduler, bool forceYielding)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(taskScheduler);

            return condition ? new WitnessOnSignal<T>(source, AsyncContext.From(taskScheduler), forceYielding) : source;
        }
    }
}
