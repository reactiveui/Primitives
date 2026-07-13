// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that combines the latest values from thirteen source sequences using a selector.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <typeparam name="T3">Element type of source 3.</typeparam>
/// <typeparam name="T4">Element type of source 4.</typeparam>
/// <typeparam name="T5">Element type of source 5.</typeparam>
/// <typeparam name="T6">Element type of source 6.</typeparam>
/// <typeparam name="T7">Element type of source 7.</typeparam>
/// <typeparam name="T8">Element type of source 8.</typeparam>
/// <typeparam name="T9">Element type of source 9.</typeparam>
/// <typeparam name="T10">Element type of source 10.</typeparam>
/// <typeparam name="T11">Element type of source 11.</typeparam>
/// <typeparam name="T12">Element type of source 12.</typeparam>
/// <typeparam name="T13">Element type of source 13.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
public sealed class
    SyncLatest13Signal<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> : IObservableAsync<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="SyncLatest13Signal{T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult}"/> class.</summary>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    public SyncLatest13Signal(
        SyncLatest13State<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> sources,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector)
    {
        Sources = sources;
        Selector = selector;
    }

    /// <summary>Gets the bundled source observables.</summary>
    private SyncLatest13State<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> Sources { get; }

    /// <summary>Gets the selector that projects the latest values.</summary>
    private Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> Selector { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        SyncLatest13Coordinator<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> subscription =
            new(observer, Sources, Selector);
        subscription.Lifecycle.LinkExternalCancellation(cancellationToken);
        return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
            subscription,
            () => subscription.SubscribeSourcesAsync(cancellationToken));
    }
}
