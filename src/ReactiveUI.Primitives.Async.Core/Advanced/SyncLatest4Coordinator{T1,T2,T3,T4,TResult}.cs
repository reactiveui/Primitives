// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates subscriptions and latest-value emission for the arity-4 <c>SyncLatest</c> operator.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <typeparam name="T3">Element type of source 3.</typeparam>
/// <typeparam name="T4">Element type of source 4.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Value1 = {Value1}, Value4 = {Value4}")]
public sealed class SyncLatest4Coordinator<T1, T2, T3, T4, TResult> : SyncLatestCoordinatorBase<TResult>
{
    /// <summary>Number of upstream sources this coordinator combines.</summary>
    private const int SourceCount = 4;

    /// <summary>Bit owned by source 1 inside the lifecycle's completion bitmask.</summary>
    private const int Source1Bit = 1;

    /// <summary>Bit owned by source 2 inside the lifecycle's completion bitmask.</summary>
    private const int Source2Bit = 1 << 1;

    /// <summary>Bit owned by source 3 inside the lifecycle's completion bitmask.</summary>
    private const int Source3Bit = 1 << 2;

    /// <summary>Bit owned by source 4 inside the lifecycle's completion bitmask.</summary>
    private const int Source4Bit = 1 << 3;

    /// <summary>0-based index of source 1 within the coordinator's subscribe dispatch.</summary>
    private const int Source1Index = 0;

    /// <summary>0-based index of source 2 within the coordinator's subscribe dispatch.</summary>
    private const int Source2Index = 1;

    /// <summary>0-based index of source 3 within the coordinator's subscribe dispatch.</summary>
    private const int Source3Index = 2;

    /// <summary>Initializes a new instance of the <see cref="SyncLatest4Coordinator{T1, T2, T3, T4, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    public SyncLatest4Coordinator(
        IObserverAsync<TResult> observer,
        SyncLatest4State<T1, T2, T3, T4> sources,
        Func<T1, T2, T3, T4, TResult> selector)
        : base(observer, SourceCount)
    {
        Sources = sources;
        Selector = selector;
    }

    /// <summary>Gets the bundled source observables.</summary>
    private SyncLatest4State<T1, T2, T3, T4> Sources { get; }

    /// <summary>Gets the selector that projects the latest values.</summary>
    private Func<T1, T2, T3, T4, TResult> Selector { get; }

    /// <summary>Gets or sets the latest value from source 1.</summary>
    private Optional<T1> Value1 { get; set; } = Optional<T1>.Empty;

    /// <summary>Gets or sets the latest value from source 2.</summary>
    private Optional<T2> Value2 { get; set; } = Optional<T2>.Empty;

    /// <summary>Gets or sets the latest value from source 3.</summary>
    private Optional<T3> Value3 { get; set; } = Optional<T3>.Empty;

    /// <summary>Gets or sets the latest value from source 4.</summary>
    private Optional<T4> Value4 { get; set; } = Optional<T4>.Empty;

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
            values.V3,
            values.V4);
        return Lifecycle.EmitDownstreamAsync(projected);
    }

    /// <inheritdoc/>
    protected override ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken) =>
        index switch
        {
            Source1Index => Sources.Source1.SubscribeAsync(new SyncLatestWitness<T1, TResult>(this, Source1Bit, value => Value1 = new(value)), cancellationToken),
            Source2Index => Sources.Source2.SubscribeAsync(new SyncLatestWitness<T2, TResult>(this, Source2Bit, value => Value2 = new(value)), cancellationToken),
            Source3Index => Sources.Source3.SubscribeAsync(new SyncLatestWitness<T3, TResult>(this, Source3Bit, value => Value3 = new(value)), cancellationToken),
            _ => Sources.Source4.SubscribeAsync(new SyncLatestWitness<T4, TResult>(this, Source4Bit, value => Value4 = new(value)), cancellationToken)
        };

    /// <summary>
    /// Reads every source's latest value into a single snapshot. Returns <see langword="false"/>
    /// until every source has produced at least one value.
    /// </summary>
    /// <param name="values">When the method returns <see langword="true"/>, the snapshot.</param>
    /// <returns><see langword="true"/> when every source has produced a value; otherwise <see langword="false"/>.</returns>
    private bool TryReadValues(out (T1 V1, T2 V2, T3 V3, T4 V4) values)
    {
        if (Value1.TryGetValue(out var value1)
            && Value2.TryGetValue(out var value2)
            && Value3.TryGetValue(out var value3)
            && Value4.TryGetValue(out var value4))
        {
            values = (value1, value2, value3, value4);
            return true;
        }

        values = default;
        return false;
    }
}
