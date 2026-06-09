// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Shared terminal-forwarding helpers for single-source sink observers. Each sink computes a
/// single result and forwards it downstream exactly once, disposing itself afterwards. Centralizing the
/// <c>try</c>/<c>finally</c> forward-then-dispose dance keeps the individual sinks to their distinguishing
/// accumulation logic and removes the otherwise-identical terminal boilerplate.
/// </summary>
internal static class SinkTerminal
{
    /// <summary>Forwards a fault to <paramref name="observer"/> and then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="error">The error to forward.</param>
    /// <param name="sink">The sink to dispose once the error has been delivered.</param>
    public static void Fault<TResult>(IObserver<TResult> observer, Exception error, IDisposable sink)
    {
        try
        {
            observer.OnError(error);
        }
        finally
        {
            sink.Dispose();
        }
    }

    /// <summary>Emits a single terminal <paramref name="value"/>, completes <paramref name="observer"/>, then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The single terminal value to emit.</param>
    /// <param name="sink">The sink to dispose once completion has been delivered.</param>
    public static void Complete<TResult>(IObserver<TResult> observer, TResult value, IDisposable sink)
    {
        try
        {
            observer.OnNext(value);
            observer.OnCompleted();
        }
        finally
        {
            sink.Dispose();
        }
    }

    /// <summary>Completes <paramref name="observer"/> without a value, then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sink">The sink to dispose once completion has been delivered.</param>
    public static void Complete<TResult>(IObserver<TResult> observer, IDisposable sink)
    {
        try
        {
            observer.OnCompleted();
        }
        finally
        {
            sink.Dispose();
        }
    }
}
