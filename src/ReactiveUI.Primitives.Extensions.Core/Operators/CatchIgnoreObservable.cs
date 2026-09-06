// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Operator that catches exceptions of a specific type and completes.</summary>
/// <typeparam name="TSource">The type of the elements in the source sequence.</typeparam>
/// <typeparam name="TException">The type of the exception to catch.</typeparam>
/// <param name="source">The source observable sequence.</param>
/// <param name="errorAction">Action to invoke when an exception of type <typeparamref name="TException"/> occurs.</param>
[System.Diagnostics.DebuggerDisplay("CatchIgnoreObservable: Source = {_source}, ErrorAction = {_errorAction}")]
public sealed class CatchIgnoreObservable<TSource, TException>(
    IObservable<TSource> source,
    Action<TException> errorAction) : IObservable<TSource>
    where TException : Exception
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<TSource> _source = InvalidOperationExceptionHelper.Check(source);

    /// <summary>The action to invoke when an exception occurs.</summary>
    private readonly Action<TException> _errorAction = InvalidOperationExceptionHelper.Check(errorAction);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TSource> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return _source.Subscribe(new CatchIgnoreWitness(observer, _errorAction));
    }

    /// <summary>Observer that catches specific exceptions.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="errorAction">The error action.</param>
    private sealed class CatchIgnoreWitness(
        IObserver<TSource> downstream,
        Action<TException> errorAction) : IObserver<TSource>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TSource value) => downstream.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (error is TException ex)
            {
                try
                {
                    errorAction(ex);
                }
                catch (Exception actionEx)
                {
                    downstream.OnError(actionEx);
                    return;
                }

                downstream.OnCompleted();
            }
            else
            {
                downstream.OnError(error);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
