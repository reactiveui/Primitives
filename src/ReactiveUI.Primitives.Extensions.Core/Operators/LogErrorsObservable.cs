// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Forwards source values verbatim, invokes the logger on error, then propagates the error downstream.</summary>
/// <typeparam name="T">Element type.</typeparam>
/// <param name="source">Upstream source.</param>
/// <param name="logger">Invoked once with the source error before downstream sees it.</param>
public sealed class LogErrorsObservable<T>(IObservable<T> source, Action<Exception> logger) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new LogErrorsWitness(observer, logger));
    }

    /// <summary>Per-subscription observer that taps errors through the logger.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="logger">Logger invoked on the error path.</param>
    private sealed class LogErrorsWitness(IObserver<T> downstream, Action<Exception> logger) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            logger(error);
            downstream.OnError(error);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
