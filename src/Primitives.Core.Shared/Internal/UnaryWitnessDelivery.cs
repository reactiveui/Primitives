// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;
#else
using ReactiveUI.Primitives.Advanced;
#endif

#if PRIMITIVES_CORE
namespace ReactiveUI.Primitives.Core.Internal;
#elif REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Shared atomic stop-flag, subscription disposal, and guarded delivery helpers for unary synchronous witnesses.</summary>
internal static class UnaryWitnessDelivery
{
    /// <summary>Determines whether <paramref name="error"/> is a fatal runtime exception that must not be caught.</summary>
    /// <param name="error">The exception to classify.</param>
    /// <returns><see langword="true"/> when <paramref name="error"/> is fatal; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsFatal(Exception error) =>
#if PRIMITIVES_CORE
        CoreFatalExceptionHelper.IsFatal(error);
#else
        FatalExceptionHelper.IsFatal(error);
#endif

    /// <summary>Marks the witness stopped and disposes the upstream subscription atomically.</summary>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="subscription">The upstream subscription slot.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Dispose(ref int stopped, ref IDisposable? subscription)
    {
        Volatile.Write(ref stopped, 1);
        SinkSubscription.Dispose(ref subscription);
    }

#if PRIMITIVES_CORE
    /// <summary>Forwards <paramref name="value"/> to <paramref name="onNext"/> while the witness has not stopped.</summary>
    /// <typeparam name="T">The observed element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="value">The value to forward.</param>
    /// <param name="onNext">The value callback.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void NextCallback<T>(ref int stopped, T value, Action<T> onNext)
    {
        if (Volatile.Read(ref stopped) == 0)
        {
            onNext(value);
        }
    }

    /// <summary>Claims termination once, invokes <paramref name="callback"/>, and disposes <paramref name="subscription"/>.</summary>
    /// <typeparam name="TState">The callback state type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="subscription">The upstream subscription slot.</param>
    /// <param name="state">The state forwarded to <paramref name="callback"/>.</param>
    /// <param name="callback">The terminal callback.</param>
    internal static void TerminateCallback<TState>(
        ref int stopped,
        ref IDisposable? subscription,
        TState state,
        Action<TState> callback)
    {
        if (Interlocked.Exchange(ref stopped, 1) != 0)
        {
            return;
        }

        try
        {
            callback(state);
        }
        finally
        {
            SinkSubscription.Dispose(ref subscription);
        }
    }
#endif

    /// <summary>Claims the terminal transition and completes <paramref name="observer"/>, disposing <paramref name="cleanup"/> afterwards.</summary>
    /// <typeparam name="T">The observer element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The disposable witness to release after forwarding completion.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Complete<T>(ref int stopped, IObserver<T> observer, IDisposable cleanup)
    {
        if (Interlocked.Exchange(ref stopped, 1) == 0)
        {
            SinkTerminal.Complete(observer, cleanup);
        }
    }

    /// <summary>Claims the terminal transition, emits <paramref name="value"/>, completes <paramref name="observer"/>, and disposes <paramref name="cleanup"/>.</summary>
    /// <typeparam name="T">The observer element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The terminal value to emit before completion.</param>
    /// <param name="cleanup">The disposable witness to release after forwarding completion.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Complete<T>(ref int stopped, IObserver<T> observer, T value, IDisposable cleanup)
    {
        if (Interlocked.Exchange(ref stopped, 1) == 0)
        {
            SinkTerminal.Complete(observer, value, cleanup);
        }
    }

    /// <summary>Claims the terminal transition, runs <paramref name="onCompleted"/>, and completes or faults <paramref name="observer"/> before disposing <paramref name="cleanup"/>.</summary>
    /// <typeparam name="T">The observer element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="onCompleted">The completion side-effect callback.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The disposable witness to release after forwarding termination.</param>
    internal static void CompleteTapped<T>(
        ref int stopped,
        Action onCompleted,
        IObserver<T> observer,
        IDisposable cleanup)
    {
        if (Interlocked.Exchange(ref stopped, 1) != 0)
        {
            return;
        }

        try
        {
            onCompleted();
        }
        catch (Exception error) when (!IsFatal(error))
        {
            SinkTerminal.Fault(observer, error, cleanup);
            return;
        }

        SinkTerminal.Complete(observer, cleanup);
    }

    /// <summary>Claims the terminal transition and faults <paramref name="observer"/>, disposing <paramref name="cleanup"/> afterwards.</summary>
    /// <typeparam name="T">The observer element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="error">The terminal error.</param>
    /// <param name="cleanup">The disposable witness to release after forwarding the error.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void Fault<T>(ref int stopped, IObserver<T> observer, Exception error, IDisposable cleanup)
    {
        if (Interlocked.Exchange(ref stopped, 1) == 0)
        {
            SinkTerminal.Fault(observer, error, cleanup);
        }
    }

    /// <summary>Claims the terminal transition, runs <paramref name="onError"/>, and faults <paramref name="observer"/> before disposing <paramref name="cleanup"/>.</summary>
    /// <typeparam name="T">The observer element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="error">The upstream error.</param>
    /// <param name="onError">The error side-effect callback.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The disposable witness to release after forwarding the error.</param>
    internal static void FaultTapped<T>(
        ref int stopped,
        Exception error,
        Action<Exception> onError,
        IObserver<T> observer,
        IDisposable cleanup)
    {
        if (Interlocked.Exchange(ref stopped, 1) != 0)
        {
            return;
        }

        try
        {
            onError(error);
        }
        catch (Exception callbackError) when (!IsFatal(callbackError))
        {
            SinkTerminal.Fault(observer, callbackError, cleanup);
            return;
        }

        SinkTerminal.Fault(observer, error, cleanup);
    }

    /// <summary>Evaluates <paramref name="accept"/> while active, forwarding accepted values or routing non-fatal exceptions to <see cref="Fault{T}"/>.</summary>
    /// <typeparam name="TSource">The element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="value">The incoming element.</param>
    /// <param name="context">The caller context passed to <paramref name="accept"/>.</param>
    /// <param name="accept">The filter predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The disposable witness to release on error or downstream failure.</param>
    internal static void NextFiltered<TSource, TContext>(
        ref int stopped,
        TSource value,
        TContext context,
        Func<TSource, TContext, bool> accept,
        IObserver<TSource> observer,
        IDisposable cleanup)
    {
        if (Volatile.Read(ref stopped) != 0)
        {
            return;
        }

        bool keep;
        try
        {
            keep = accept(value, context);
        }
        catch (Exception error) when (!IsFatal(error))
        {
            Fault(ref stopped, observer, error, cleanup);
            return;
        }

        if (keep)
        {
            SinkDelivery.Next(observer, value, cleanup);
        }
    }

    /// <summary>Evaluates <paramref name="project"/> while active, forwarding the projected value or routing non-fatal exceptions to <see cref="Fault{T}"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="value">The incoming element.</param>
    /// <param name="context">The caller context passed to <paramref name="project"/>.</param>
    /// <param name="project">The element projection.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The disposable witness to release on error or downstream failure.</param>
    internal static void NextProjected<TSource, TContext, TResult>(
        ref int stopped,
        TSource value,
        TContext context,
        Func<TSource, TContext, TResult> project,
        IObserver<TResult> observer,
        IDisposable cleanup)
    {
        if (Volatile.Read(ref stopped) != 0)
        {
            return;
        }

        TResult projected;
        try
        {
            projected = project(value, context);
        }
        catch (Exception error) when (!IsFatal(error))
        {
            Fault(ref stopped, observer, error, cleanup);
            return;
        }

        SinkDelivery.Next(observer, projected, cleanup);
    }

    /// <summary>Invokes a stateful side effect while active, forwarding <paramref name="value"/> unchanged or routing non-fatal exceptions to <see cref="Fault{T}"/>.</summary>
    /// <typeparam name="TSource">The element type.</typeparam>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="value">The incoming element.</param>
    /// <param name="tap">The state and side-effect action pair.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The disposable witness to release on error or downstream failure.</param>
    internal static void NextTapped<TSource, TState>(
        ref int stopped,
        TSource value,
        StateCallbackPair<TState, Action<TState, TSource>> tap,
        IObserver<TSource> observer,
        IDisposable cleanup)
    {
        if (Volatile.Read(ref stopped) != 0)
        {
            return;
        }

        try
        {
            tap.Callback(tap.State, value);
        }
        catch (Exception error) when (!IsFatal(error))
        {
            Fault(ref stopped, observer, error, cleanup);
            return;
        }

        SinkDelivery.Next(observer, value, cleanup);
    }

    /// <summary>Evaluates <paramref name="evaluate"/> while active, forwarding matching projected values or routing non-fatal exceptions to <see cref="Fault{T}"/>.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="stopped">The witness stop flag.</param>
    /// <param name="value">The incoming element.</param>
    /// <param name="context">The caller context passed to <paramref name="evaluate"/>.</param>
    /// <param name="evaluate">Evaluates projection and acceptance for <paramref name="value"/>.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The disposable witness to release on error or downstream failure.</param>
    internal static void NextFilteredProjection<TSource, TContext, TResult>(
        ref int stopped,
        TSource value,
        TContext context,
        Func<TSource, TContext, ProjectedOutcome<TResult>> evaluate,
        IObserver<TResult> observer,
        IDisposable cleanup)
    {
        if (Volatile.Read(ref stopped) != 0)
        {
            return;
        }

        ProjectedOutcome<TResult> outcome;
        try
        {
            outcome = evaluate(value, context);
        }
        catch (Exception error) when (!IsFatal(error))
        {
            Fault(ref stopped, observer, error, cleanup);
            return;
        }

        if (outcome.HasValue)
        {
            SinkDelivery.Next(observer, outcome.Value, cleanup);
        }
    }
}
