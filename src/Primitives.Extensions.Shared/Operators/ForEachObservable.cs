// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Operators;
#else
namespace ReactiveUI.Primitives.Extensions.Operators;
#endif

/// <summary>
/// Flattening operator that subscribes to an upstream sequence of
/// <see cref="IEnumerable{T}"/> batches and emits each contained element
/// individually. Replaces the
/// <c>Observable.Create&lt;T&gt;(o =&gt; source.ObserveOnSafe(s).Subscribe(v =&gt; o.FastForEach(v)))</c>
/// pattern with a single operator + observer pair.
/// </summary>
/// <typeparam name="T">The element type emitted to the downstream observer.</typeparam>
/// <param name="source">The source observable of enumerables.</param>
/// <param name="scheduler">An optional scheduler used to marshal source notifications onto.</param>
internal sealed class ForEachObservable<T>(
    IObservable<IEnumerable<T>> source,
    ISequencer? scheduler) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var observed = scheduler is null ? source : new ObserveOnObservable<IEnumerable<T>>(source, scheduler);
        return observed.Subscribe(new ForEachWitness(observer));
    }

    /// <summary>Forwarding observer that fans each batch element out to the downstream.</summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class ForEachWitness(IObserver<T> downstream) : IObserver<IEnumerable<T>>
    {
        /// <inheritdoc/>
        public void OnNext(IEnumerable<T> value)
        {
            if (value is null)
            {
                return;
            }

            downstream.FastForEach(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
