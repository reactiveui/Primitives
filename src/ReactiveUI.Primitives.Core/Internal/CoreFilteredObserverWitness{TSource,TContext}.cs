// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core.Internal;

namespace ReactiveUI.Primitives.Internal;

/// <summary>Reusable observer witness that filters source items using a caller-supplied context without closure allocation.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TContext">The caller context passed to <paramref name="accept"/>.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="context">The caller context.</param>
/// <param name="accept">The non-capturing filter delegate.</param>
internal sealed class CoreFilteredObserverWitness<TSource, TContext>(
    IObserver<TSource> observer,
    TContext context,
    Func<TSource, TContext, bool> accept) : IObserver<TSource>, IDisposable
{
    /// <summary>The upstream subscription handle.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed atomically via UnaryWitnessDelivery.Dispose.")]
    private IDisposable? _subscription;

    /// <summary>Non-zero once a terminal notification has been forwarded or the sink disposed.</summary>
    private int _stopped;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => UnaryWitnessDelivery.Dispose(ref _stopped, ref _subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => UnaryWitnessDelivery.Complete(ref _stopped, observer, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => UnaryWitnessDelivery.Fault(ref _stopped, observer, error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(TSource value) =>
        UnaryWitnessDelivery.NextFiltered(ref _stopped, value, context, accept, observer, this);

    /// <summary>Assigns the upstream subscription, disposing it immediately if already stopped.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);
}
