// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Shared terminal-forwarding helpers for single-source sink observers. Each sink computes a
/// single result and forwards it downstream exactly once, disposing itself afterwards. Centralizing the
/// forward-then-dispose <c>using</c> scope — and the <c>done</c> latch that guards it — keeps the
/// individual sinks to their distinguishing accumulation logic and removes the otherwise-identical terminal boilerplate.
/// </summary>
public static class SinkTerminal
{
    /// <summary>Forwards a fault to <paramref name="observer"/> and then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="error">The error to forward.</param>
    /// <param name="sink">The sink to dispose once the error has been delivered.</param>
    public static void Fault<TResult>(IObserver<TResult> observer, Exception error, IDisposable sink)
    {
        using (sink)
        {
            observer.OnError(error);
        }
    }

    /// <summary>Forwards a fault exactly once, latching <paramref name="done"/>, then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="error">The error to forward.</param>
    /// <param name="sink">The sink to dispose once the error has been delivered.</param>
    /// <param name="done">The caller-owned terminal latch; ignored once already set.</param>
    public static void Fault<TResult>(IObserver<TResult> observer, Exception error, IDisposable sink, ref bool done)
    {
        if (done)
        {
            return;
        }

        done = true;
        Fault(observer, error, sink);
    }

    /// <summary>Emits a single terminal <paramref name="value"/>, completes <paramref name="observer"/>, then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The single terminal value to emit.</param>
    /// <param name="sink">The sink to dispose once completion has been delivered.</param>
    public static void Complete<TResult>(IObserver<TResult> observer, TResult value, IDisposable sink)
    {
        using (sink)
        {
            observer.OnNext(value);
            observer.OnCompleted();
        }
    }

    /// <summary>Emits a single terminal <paramref name="value"/> and completes exactly once, latching <paramref name="done"/>, then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The single terminal value to emit.</param>
    /// <param name="sink">The sink to dispose once completion has been delivered.</param>
    /// <param name="done">The caller-owned terminal latch; ignored once already set.</param>
    public static void Complete<TResult>(IObserver<TResult> observer, TResult value, IDisposable sink, ref bool done)
    {
        if (done)
        {
            return;
        }

        done = true;
        Complete(observer, value, sink);
    }

    /// <summary>Completes <paramref name="observer"/> without a value, then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sink">The sink to dispose once completion has been delivered.</param>
    public static void Complete<TResult>(IObserver<TResult> observer, IDisposable sink)
    {
        using (sink)
        {
            observer.OnCompleted();
        }
    }

    /// <summary>Completes <paramref name="observer"/> without a value exactly once, latching <paramref name="done"/>, then disposes <paramref name="sink"/>.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sink">The sink to dispose once completion has been delivered.</param>
    /// <param name="done">The caller-owned terminal latch; ignored once already set.</param>
    public static void Complete<TResult>(IObserver<TResult> observer, IDisposable sink, ref bool done)
    {
        if (done)
        {
            return;
        }

        done = true;
        Complete(observer, sink);
    }
}
