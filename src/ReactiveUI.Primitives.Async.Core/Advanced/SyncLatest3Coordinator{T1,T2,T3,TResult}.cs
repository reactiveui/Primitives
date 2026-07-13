// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates subscriptions and latest-value emission for the arity-3 <c>SyncLatest</c> operator.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <typeparam name="T3">Element type of source 3.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
public sealed class SyncLatest3Coordinator<T1, T2, T3, TResult> : SyncLatestCoordinatorBase<TResult>
{
    /// <summary>Number of upstream sources this coordinator combines.</summary>
    private const int SourceCount = 3;

    /// <summary>Bit owned by source 1 inside the lifecycle's completion bitmask.</summary>
    private const int Source1Bit = 1;

    /// <summary>Bit owned by source 2 inside the lifecycle's completion bitmask.</summary>
    private const int Source2Bit = 1 << 1;

    /// <summary>Bit owned by source 3 inside the lifecycle's completion bitmask.</summary>
    private const int Source3Bit = 1 << 2;

    /// <summary>0-based index of source 1 within the coordinator's subscribe dispatch.</summary>
    private const int Source1Index = 0;

    /// <summary>0-based index of source 2 within the coordinator's subscribe dispatch.</summary>
    private const int Source2Index = 1;

    /// <summary>Initializes a new instance of the <see cref="SyncLatest3Coordinator{T1, T2, T3, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    public SyncLatest3Coordinator(
        IObserverAsync<TResult> observer,
        SyncLatest3State<T1, T2, T3> sources,
        Func<T1, T2, T3, TResult> selector)
        : base(observer, SourceCount)
    {
        Sources = sources;
        Selector = selector;
    }

    /// <summary>Gets the bundled source observables.</summary>
    private SyncLatest3State<T1, T2, T3> Sources { get; }

    /// <summary>Gets the selector that projects the latest values.</summary>
    private Func<T1, T2, T3, TResult> Selector { get; }

    /// <summary>Gets or sets the latest value from source 1.</summary>
    private Optional<T1> Value1 { get; set; } = Optional<T1>.Empty;

    /// <summary>Gets or sets the latest value from source 2.</summary>
    private Optional<T2> Value2 { get; set; } = Optional<T2>.Empty;

    /// <summary>Gets or sets the latest value from source 3.</summary>
    private Optional<T3> Value3 { get; set; } = Optional<T3>.Empty;

    /// <inheritdoc/>
    internal override ValueTask EmitLatestAsync()
    {
        if (!TryReadValues(out var values))
        {
            return default;
        }

        var projected = Selector(
            values.V1,
            values.V2,
            values.V3);
        return Lifecycle.EmitDownstreamAsync(projected);
    }

    /// <inheritdoc/>
    [SuppressMessage(
        "Major Code Smell",
        "S1541:Methods and properties should not be too complex",
        Justification = "Switch arm per source; the high arm count is the dispatch surface.")]
    protected override ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken) =>
        index switch
        {
            Source1Index => Sources.Source1.SubscribeAsync(new SyncLatestWitness<T1, TResult>(this, Source1Bit, value => Value1 = new(value)), cancellationToken),
            Source2Index => Sources.Source2.SubscribeAsync(new SyncLatestWitness<T2, TResult>(this, Source2Bit, value => Value2 = new(value)), cancellationToken),
            _ => Sources.Source3.SubscribeAsync(new SyncLatestWitness<T3, TResult>(this, Source3Bit, value => Value3 = new(value)), cancellationToken)
        };

    /// <summary>
    /// Reads every source's latest value into a single snapshot. Returns <see langword="false"/>
    /// until every source has produced at least one value.
    /// </summary>
    /// <param name="values">When the method returns <see langword="true"/>, the snapshot.</param>
    /// <returns><see langword="true"/> when every source has produced a value; otherwise <see langword="false"/>.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S1541:Methods and properties should not be too complex",
        Justification = "Short-circuited checks over every source's Optional are the snapshot semantic.")]
    private bool TryReadValues(out (T1 V1, T2 V2, T3 V3) values)
    {
        if (Value1.TryGetValue(out var value1)
            && Value2.TryGetValue(out var value2)
            && Value3.TryGetValue(out var value3))
        {
            values = (value1, value2, value3);
            return true;
        }

        values = default;
        return false;
    }
}
