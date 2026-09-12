// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Chains two one-shot observable projections and forwards errors from either stage.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TMid">The intermediate element type produced by the first projection.</typeparam>
/// <typeparam name="TResult">The final element type produced by the second projection.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="first">First projection: source element → intermediate observable.</param>
/// <param name="second">Second projection: intermediate element → result observable.</param>
public sealed class SelectManyThenObservable<TSource, TMid, TResult>(
    IObservable<TSource> source,
    Func<TSource, IObservable<TMid>> first,
    Func<TMid, IObservable<TResult>> second) : IObservable<TResult>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(first);
        InvalidOperationExceptionHelper.ThrowIfNull(second);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new SourceWitness(observer, first, second));
    }

    /// <summary>Subscribes the reusable intermediate observer to the first projection.</summary>
    private sealed class SourceWitness : IObserver<TSource>
    {
        /// <summary>The downstream observer that ultimately receives <typeparamref name="TResult"/> values.</summary>
        private readonly IObserver<TResult> _downstream;

        /// <summary>First projection delegate.</summary>
        private readonly Func<TSource, IObservable<TMid>> _first;

        /// <summary>Pre-allocated intermediate observer shared across every source emission.</summary>
        private readonly MidWitness _midObserver;

        /// <summary>Initializes a new instance of the <see cref="SourceWitness"/> class and primes the reusable mid observer.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="first">First projection delegate.</param>
        /// <param name="second">Second projection delegate.</param>
        public SourceWitness(
            IObserver<TResult> downstream,
            Func<TSource, IObservable<TMid>> first,
            Func<TMid, IObservable<TResult>> second)
        {
            _downstream = downstream;
            _first = first;
            _midObserver = new(downstream, second);
        }

        /// <inheritdoc/>
        public void OnNext(TSource value)
        {
            try
            {
                _ = _first(value).Subscribe(_midObserver);
            }
            catch (Exception ex)
            {
                _downstream.OnError(ex);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _downstream.OnCompleted();
    }

    /// <summary>Receives the intermediate value, applies <c>second</c>, and subscribes the resulting observable directly to <c>downstream</c> — no separate final-stage observer needed.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="second">Second projection delegate.</param>
    private sealed class MidWitness(
        IObserver<TResult> downstream,
        Func<TMid, IObservable<TResult>> second) : IObserver<TMid>
    {
        /// <inheritdoc/>
        public void OnNext(TMid value)
        {
            try
            {
                _ = second(value).Subscribe(downstream);
            }
            catch (Exception ex)
            {
                downstream.OnError(ex);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
