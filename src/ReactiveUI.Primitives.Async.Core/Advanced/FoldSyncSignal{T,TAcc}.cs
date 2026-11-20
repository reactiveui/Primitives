// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Accumulates source elements synchronously from an initial seed and emits each running value.</summary>
/// <typeparam name="T">The element type of the source sequence.</typeparam>
/// <typeparam name="TAcc">The accumulator type.</typeparam>
[System.Diagnostics.DebuggerDisplay("FoldSyncSignal: Source = {_source}, Seed = {_seed}")]
public sealed class FoldSyncSignal<T, TAcc> : IObservableAsync<TAcc>
{
    /// <summary>The source observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The initial accumulator value.</summary>
    private readonly TAcc _seed;

    /// <summary>The synchronous accumulator.</summary>
    private readonly Func<TAcc, T, TAcc> _accumulator;

    /// <summary>Initializes a new instance of the <see cref="FoldSyncSignal{T, TAcc}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The synchronous accumulator.</param>
    public FoldSyncSignal(
        IObservableAsync<T> source,
        TAcc seed,
        Func<TAcc, T, TAcc> accumulator)
    {
        ArgumentExceptionHelper.ThrowIfNull(accumulator);
        _source = source;
        _seed = seed;
        _accumulator = accumulator;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<TAcc>.SubscribeAsync(
        IObserverAsync<TAcc> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new FoldSyncWitness<T, TAcc>(observer, _seed, _accumulator, cancellationToken),
            observer,
            cancellationToken);
}
