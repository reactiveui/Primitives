// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Inner observer that relays projected values to a flat-map coordinator.</summary>
/// <typeparam name="TResult">The result element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FlatMapWitness: Coordinator = {Coordinator}")]
public sealed class FlatMapWitness<TResult> : WitnessAsync<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="FlatMapWitness{TResult}"/> class.</summary>
    /// <param name="coordinator">The flat-map coordinator.</param>
    public FlatMapWitness(FlatMapCoordinator<TResult> coordinator) => Coordinator = coordinator;

    /// <summary>Gets the flat-map coordinator.</summary>
    private FlatMapCoordinator<TResult> Coordinator { get; }

    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(TResult value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return Coordinator.RelayNextAsync(value);
    }

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return Coordinator.RelayErrorAsync(error);
    }

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) =>
        Coordinator.CompleteInnerAsync(result);
}
