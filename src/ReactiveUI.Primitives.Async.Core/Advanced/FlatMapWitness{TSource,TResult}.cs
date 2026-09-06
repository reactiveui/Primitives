// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Outer observer that projects source values to inner observables.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The result element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FlatMapWitness: Coordinator = {Coordinator}, SyncSelector = {SyncSelector}, AsyncSelector = {AsyncSelector}")]
public sealed class FlatMapWitness<TSource, TResult> : WitnessAsync<TSource>
{
    /// <summary>Initializes a new instance of the <see cref="FlatMapWitness{TSource,TResult}"/> class.</summary>
    /// <param name="coordinator">The flat-map coordinator.</param>
    /// <param name="syncSelector">The synchronous selector.</param>
    /// <param name="asyncSelector">The asynchronous selector.</param>
    public FlatMapWitness(
        FlatMapCoordinator<TResult> coordinator,
        Func<TSource, IObservableAsync<TResult>>? syncSelector,
        Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>>? asyncSelector)
    {
        Coordinator = coordinator;
        SyncSelector = syncSelector;
        AsyncSelector = asyncSelector;
    }

    /// <summary>Gets the flat-map coordinator.</summary>
    private FlatMapCoordinator<TResult> Coordinator { get; }

    /// <summary>Gets the synchronous selector.</summary>
    private Func<TSource, IObservableAsync<TResult>>? SyncSelector { get; }

    /// <summary>Gets the asynchronous selector.</summary>
    private Func<TSource, CancellationToken, ValueTask<IObservableAsync<TResult>>>? AsyncSelector { get; }

    /// <inheritdoc/>
    protected override async ValueTask OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
    {
        var inner = SyncSelector is not null
            ? SyncSelector(value)
            : await AsyncSelector!(value, cancellationToken).ConfigureAwait(false);

        await Coordinator.SubscribeInnerAsync(inner).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return Coordinator.RelayErrorAsync(error);
    }

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        Coordinator.CompleteOuterAsync(result);
}
