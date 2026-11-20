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

/// <summary>Encapsulates the upstream subscription handle and atomic stop flag for a unary synchronous witness.</summary>
internal record struct UnaryWitnessSlot
{
    /// <summary>The upstream subscription handle.</summary>
    private IDisposable? _subscription;

    /// <summary>Non-zero once stopped.</summary>
    private int _stopped;

    /// <summary>Assigns the upstream subscription, disposing it immediately if already stopped.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Attach(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <summary>Marks this slot stopped and disposes the upstream subscription.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Dispose() => UnaryWitnessDelivery.Dispose(ref _stopped, ref _subscription);

    /// <summary>Completes <paramref name="observer"/> at most once and disposes <paramref name="cleanup"/>.</summary>
    /// <typeparam name="T">The observer element type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The witness to dispose after forwarding completion.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Complete<T>(IObserver<T> observer, IDisposable cleanup) =>
        UnaryWitnessDelivery.Complete(ref _stopped, observer, cleanup);

    /// <summary>Faults <paramref name="observer"/> at most once and disposes <paramref name="cleanup"/>.</summary>
    /// <typeparam name="T">The observer element type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="error">The terminal error.</param>
    /// <param name="cleanup">The witness to dispose after forwarding the error.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Fault<T>(IObserver<T> observer, Exception error, IDisposable cleanup) =>
        UnaryWitnessDelivery.Fault(ref _stopped, observer, error, cleanup);

    /// <summary>Forwards <paramref name="value"/> when accepted by <paramref name="accept"/> while active.</summary>
    /// <typeparam name="TSource">The element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <param name="value">The incoming element.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="accept">The filter predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The witness to dispose on fault.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void NextFiltered<TSource, TContext>(
        TSource value,
        TContext context,
        Func<TSource, TContext, bool> accept,
        IObserver<TSource> observer,
        IDisposable cleanup) =>
        UnaryWitnessDelivery.NextFiltered(ref _stopped, value, context, accept, observer, cleanup);

    /// <summary>Projects and forwards <paramref name="value"/> via <paramref name="project"/> while active.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="value">The incoming element.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="project">The projection function.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The witness to dispose on fault.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void NextProjected<TSource, TContext, TResult>(
        TSource value,
        TContext context,
        Func<TSource, TContext, TResult> project,
        IObserver<TResult> observer,
        IDisposable cleanup) =>
        UnaryWitnessDelivery.NextProjected(ref _stopped, value, context, project, observer, cleanup);

    /// <summary>Evaluates <paramref name="evaluate"/> and forwards matching projections while active.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TContext">The caller context type.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="value">The incoming element.</param>
    /// <param name="context">The caller context.</param>
    /// <param name="evaluate">The fused evaluation function.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cleanup">The witness to dispose on fault.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void NextFilteredProjection<TSource, TContext, TResult>(
        TSource value,
        TContext context,
        Func<TSource, TContext, ProjectedOutcome<TResult>> evaluate,
        IObserver<TResult> observer,
        IDisposable cleanup) =>
        UnaryWitnessDelivery.NextFilteredProjection(ref _stopped, value, context, evaluate, observer, cleanup);
}
