// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Fused projection operators that combine filtering or switching with a projection in one sink.</summary>
public static partial class LinqExtensions
{
    /// <summary>Fused projection operators for an observable source sequence.</summary>
    /// <typeparam name="TIn">The source element type.</typeparam>
    /// <param name="source">The source observable.</param>
    extension<TIn>(IObservable<TIn> source)
    {
        /// <summary>Projects each source value to an inner observable and mirrors only the latest one - a single fused sink in place of <c>Select(selector).Switch()</c>.</summary>
        /// <typeparam name="TOut">The element type of the projected inner observables.</typeparam>
        /// <param name="selector">Projects each source value to an inner observable.</param>
        /// <returns>An observable that mirrors the latest projected inner observable.</returns>
        /// <remarks>Null switches the active subscription too; use SwitchSelect to ignore nulls and keep the current subscription.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TOut> SwitchMap<TOut>(Func<TIn, IObservable<TOut>> selector) =>
            SwitchMapSignal<TIn, TOut>.Create(source, selector);

        /// <summary>Projects each value to a value-and-flag pair and forwards the value only when the flag is true.</summary>
        /// <typeparam name="TOut">The forwarded element type.</typeparam>
        /// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
        /// <returns>An observable of the chosen values.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TOut> Choose<TOut>(Func<TIn, (bool HasValue, TOut Value)> chooser) =>
            ChooseSignal<TIn, TOut>.Create(source, chooser);
    }

    /// <summary>Fused projection operators for an observable source sequence of nullable values.</summary>
    /// <typeparam name="TSource">The (nullable) source element type.</typeparam>
    /// <param name="source">The source observable.</param>
    extension<TSource>(IObservable<TSource?> source)
    {
        /// <summary>
        /// Filters out null source values, projects each remaining value to an inner observable, and mirrors only the
        /// latest inner observable - a single fused sink in place of <c>WhereNotNull().Select(selector).Switch()</c>.
        /// </summary>
        /// <typeparam name="TResult">The element type of the projected inner observables.</typeparam>
        /// <param name="selector">Projects each non-null source value to an inner observable.</param>
        /// <returns>An observable that mirrors the latest projected inner observable.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<TResult> SwitchSelect<TResult>(Func<TSource, IObservable<TResult>> selector) =>
            SwitchMapSignal<TSource, TResult>.CreateSkipNull(source, selector);
    }
}
