// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>
/// Fused projection operators: <c>Choose</c> (filter + map in one sink), <c>SwitchMap</c> (map-to-inner +
/// switch-to-latest in one sink) and <c>SwitchSelect</c> (the same, skipping null source values).
/// </summary>
public static partial class LinqExtensions
{
    /// <summary>Fused projection operators for an observable source sequence.</summary>
    /// <param name="source">The source observable.</param>
    /// <typeparam name="TIn">The source element type.</typeparam>
    extension<TIn>(IObservable<TIn> source)
    {
        /// <summary>
        /// Projects each source value to an inner observable and mirrors only the latest one — a single fused
        /// sink in place of <c>Select(selector).Switch()</c>.
        /// </summary>
        /// <typeparam name="TOut">The element type of the projected inner observables.</typeparam>
        /// <param name="selector">Projects each source value to an inner observable.</param>
        /// <returns>An observable that mirrors the latest projected inner observable.</returns>
        /// <remarks>
        /// Every source value switches, a null among them included. Skipping nulls, which leaves the active
        /// inner subscription in place, is <see cref="SwitchSelect{TSource, TResult}"/> instead.
        /// </remarks>
        public IObservable<TOut> SwitchMap<TOut>(Func<TIn, IObservable<TOut>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new SwitchMapSignal<TIn, TOut>(source, selector);
        }

        /// <summary>
        /// Maps each source value to a <c>(HasValue, Value)</c> pair and forwards only the values whose
        /// <c>HasValue</c> is <see langword="true"/> — a single fused sink in place of <c>Where(...).Select(...)</c>.
        /// Unlike a <c>TOut?</c>-returning projection, the explicit flag lets a non-nullable value type be skipped.
        /// </summary>
        /// <typeparam name="TOut">The forwarded element type.</typeparam>
        /// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
        /// <returns>An observable of the chosen values.</returns>
        public IObservable<TOut> Choose<TOut>(Func<TIn, (bool HasValue, TOut Value)> chooser)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(chooser);

            return new ChooseSignal<TIn, TOut>(source, chooser);
        }
    }

    /// <summary>Fused projection operators for an observable source sequence of nullable values.</summary>
    /// <param name="source">The source observable.</param>
    /// <typeparam name="TSource">The (nullable) source element type.</typeparam>
    extension<TSource>(IObservable<TSource?> source)
    {
        /// <summary>
        /// Filters out null source values, projects each remaining value to an inner observable, and mirrors only the
        /// latest inner observable — a single fused sink in place of <c>WhereNotNull().Select(selector).Switch()</c>.
        /// </summary>
        /// <typeparam name="TResult">The element type of the projected inner observables.</typeparam>
        /// <param name="selector">Projects each non-null source value to an inner observable.</param>
        /// <returns>An observable that mirrors the latest projected inner observable.</returns>
        public IObservable<TResult> SwitchSelect<TResult>(Func<TSource, IObservable<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new SwitchMapSignal<TSource, TResult>(source!, selector, skipNullSources: true);
        }
    }

    /// <summary>A fused filter + map observable.</summary>
    /// <typeparam name="TIn">The source element type.</typeparam>
    /// <typeparam name="TOut">The forwarded element type.</typeparam>
    /// <param name="source">The source observable whose values are filtered and mapped.</param>
    /// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
    private sealed class ChooseSignal<TIn, TOut>(
        IObservable<TIn> source,
        Func<TIn, (bool HasValue, TOut Value)> chooser) : IObservable<TOut>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<TOut> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return source.Subscribe(new Sink(observer, chooser));
        }

        /// <summary>Applies the chooser to each value and forwards only the chosen ones.</summary>
        /// <param name="downstream">The downstream observer that receives the chosen values.</param>
        /// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
        private sealed class Sink(IObserver<TOut> downstream, Func<TIn, (bool HasValue, TOut Value)> chooser) : IObserver<TIn>
        {
            /// <inheritdoc/>
            public void OnNext(TIn value)
            {
                (bool HasValue, TOut Value) result;
                try
                {
                    result = chooser(value);
                }
                catch (Exception ex)
                {
                    downstream.OnError(ex);
                    return;
                }

                if (!result.HasValue)
                {
                    return;
                }

                downstream.OnNext(result.Value);
            }

            /// <inheritdoc/>
            public void OnError(Exception error) => downstream.OnError(error);

            /// <inheritdoc/>
            public void OnCompleted() => downstream.OnCompleted();
        }
    }
}
