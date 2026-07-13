// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates subscriptions and latest-value emission for the arity-14 <c>SyncLatest</c> operator.</summary>
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
/// <typeparam name="T14">Element type of source 14.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
public sealed class
    SyncLatest14Coordinator<
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        TResult> : SyncLatestCoordinatorBase<TResult>
{
    /// <summary>Number of upstream sources this coordinator combines.</summary>
    private const int SourceCount = 14;

    /// <summary>Bit owned by source 1 inside the lifecycle's completion bitmask.</summary>
    private const int Source1Bit = 1;

    /// <summary>Bit owned by source 2 inside the lifecycle's completion bitmask.</summary>
    private const int Source2Bit = 1 << 1;

    /// <summary>Bit owned by source 3 inside the lifecycle's completion bitmask.</summary>
    private const int Source3Bit = 1 << 2;

    /// <summary>Bit owned by source 4 inside the lifecycle's completion bitmask.</summary>
    private const int Source4Bit = 1 << 3;

    /// <summary>Bit owned by source 5 inside the lifecycle's completion bitmask.</summary>
    private const int Source5Bit = 1 << 4;

    /// <summary>Bit owned by source 6 inside the lifecycle's completion bitmask.</summary>
    private const int Source6Bit = 1 << 5;

    /// <summary>Bit owned by source 7 inside the lifecycle's completion bitmask.</summary>
    private const int Source7Bit = 1 << 6;

    /// <summary>Bit owned by source 8 inside the lifecycle's completion bitmask.</summary>
    private const int Source8Bit = 1 << 7;

    /// <summary>Bit owned by source 9 inside the lifecycle's completion bitmask.</summary>
    private const int Source9Bit = 1 << 8;

    /// <summary>Bit owned by source 10 inside the lifecycle's completion bitmask.</summary>
    private const int Source10Bit = 1 << 9;

    /// <summary>Bit owned by source 11 inside the lifecycle's completion bitmask.</summary>
    private const int Source11Bit = 1 << 10;

    /// <summary>Bit owned by source 12 inside the lifecycle's completion bitmask.</summary>
    private const int Source12Bit = 1 << 11;

    /// <summary>Bit owned by source 13 inside the lifecycle's completion bitmask.</summary>
    private const int Source13Bit = 1 << 12;

    /// <summary>Bit owned by source 14 inside the lifecycle's completion bitmask.</summary>
    private const int Source14Bit = 1 << 13;

    /// <summary>0-based index of source 1 within the coordinator's subscribe dispatch.</summary>
    private const int Source1Index = 0;

    /// <summary>0-based index of source 2 within the coordinator's subscribe dispatch.</summary>
    private const int Source2Index = 1;

    /// <summary>0-based index of source 3 within the coordinator's subscribe dispatch.</summary>
    private const int Source3Index = 2;

    /// <summary>0-based index of source 4 within the coordinator's subscribe dispatch.</summary>
    private const int Source4Index = 3;

    /// <summary>0-based index of source 5 within the coordinator's subscribe dispatch.</summary>
    private const int Source5Index = 4;

    /// <summary>0-based index of source 6 within the coordinator's subscribe dispatch.</summary>
    private const int Source6Index = 5;

    /// <summary>0-based index of source 7 within the coordinator's subscribe dispatch.</summary>
    private const int Source7Index = 6;

    /// <summary>0-based index of source 8 within the coordinator's subscribe dispatch.</summary>
    private const int Source8Index = 7;

    /// <summary>0-based index of source 9 within the coordinator's subscribe dispatch.</summary>
    private const int Source9Index = 8;

    /// <summary>0-based index of source 10 within the coordinator's subscribe dispatch.</summary>
    private const int Source10Index = 9;

    /// <summary>0-based index of source 11 within the coordinator's subscribe dispatch.</summary>
    private const int Source11Index = 10;

    /// <summary>0-based index of source 12 within the coordinator's subscribe dispatch.</summary>
    private const int Source12Index = 11;

    /// <summary>0-based index of source 13 within the coordinator's subscribe dispatch.</summary>
    private const int Source13Index = 12;

    /// <summary>Initializes a new instance of the <see cref="SyncLatest14Coordinator{T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    public SyncLatest14Coordinator(
        IObserverAsync<TResult> observer,
        SyncLatest14State<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> sources,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> selector)
        : base(observer, SourceCount)
    {
        Sources = sources;
        Selector = selector;
    }

    /// <summary>Gets the bundled source observables.</summary>
    private SyncLatest14State<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> Sources { get; }

    /// <summary>Gets the selector that projects the latest values.</summary>
    private Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, TResult> Selector { get; }

    /// <summary>Gets or sets the latest value from source 1.</summary>
    private Optional<T1> Value1 { get; set; } = Optional<T1>.Empty;

    /// <summary>Gets or sets the latest value from source 2.</summary>
    private Optional<T2> Value2 { get; set; } = Optional<T2>.Empty;

    /// <summary>Gets or sets the latest value from source 3.</summary>
    private Optional<T3> Value3 { get; set; } = Optional<T3>.Empty;

    /// <summary>Gets or sets the latest value from source 4.</summary>
    private Optional<T4> Value4 { get; set; } = Optional<T4>.Empty;

    /// <summary>Gets or sets the latest value from source 5.</summary>
    private Optional<T5> Value5 { get; set; } = Optional<T5>.Empty;

    /// <summary>Gets or sets the latest value from source 6.</summary>
    private Optional<T6> Value6 { get; set; } = Optional<T6>.Empty;

    /// <summary>Gets or sets the latest value from source 7.</summary>
    private Optional<T7> Value7 { get; set; } = Optional<T7>.Empty;

    /// <summary>Gets or sets the latest value from source 8.</summary>
    private Optional<T8> Value8 { get; set; } = Optional<T8>.Empty;

    /// <summary>Gets or sets the latest value from source 9.</summary>
    private Optional<T9> Value9 { get; set; } = Optional<T9>.Empty;

    /// <summary>Gets or sets the latest value from source 10.</summary>
    private Optional<T10> Value10 { get; set; } = Optional<T10>.Empty;

    /// <summary>Gets or sets the latest value from source 11.</summary>
    private Optional<T11> Value11 { get; set; } = Optional<T11>.Empty;

    /// <summary>Gets or sets the latest value from source 12.</summary>
    private Optional<T12> Value12 { get; set; } = Optional<T12>.Empty;

    /// <summary>Gets or sets the latest value from source 13.</summary>
    private Optional<T13> Value13 { get; set; } = Optional<T13>.Empty;

    /// <summary>Gets or sets the latest value from source 14.</summary>
    private Optional<T14> Value14 { get; set; } = Optional<T14>.Empty;

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
            values.V4,
            values.V5,
            values.V6,
            values.V7,
            values.V8,
            values.V9,
            values.V10,
            values.V11,
            values.V12,
            values.V13,
            values.V14);
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
            Source3Index => Sources.Source3.SubscribeAsync(new SyncLatestWitness<T3, TResult>(this, Source3Bit, value => Value3 = new(value)), cancellationToken),
            Source4Index => Sources.Source4.SubscribeAsync(new SyncLatestWitness<T4, TResult>(this, Source4Bit, value => Value4 = new(value)), cancellationToken),
            Source5Index => Sources.Source5.SubscribeAsync(new SyncLatestWitness<T5, TResult>(this, Source5Bit, value => Value5 = new(value)), cancellationToken),
            Source6Index => Sources.Source6.SubscribeAsync(new SyncLatestWitness<T6, TResult>(this, Source6Bit, value => Value6 = new(value)), cancellationToken),
            Source7Index => Sources.Source7.SubscribeAsync(new SyncLatestWitness<T7, TResult>(this, Source7Bit, value => Value7 = new(value)), cancellationToken),
            Source8Index => Sources.Source8.SubscribeAsync(new SyncLatestWitness<T8, TResult>(this, Source8Bit, value => Value8 = new(value)), cancellationToken),
            Source9Index => Sources.Source9.SubscribeAsync(new SyncLatestWitness<T9, TResult>(this, Source9Bit, value => Value9 = new(value)), cancellationToken),
            Source10Index => Sources.Source10.SubscribeAsync(new SyncLatestWitness<T10, TResult>(this, Source10Bit, value => Value10 = new(value)), cancellationToken),
            Source11Index => Sources.Source11.SubscribeAsync(new SyncLatestWitness<T11, TResult>(this, Source11Bit, value => Value11 = new(value)), cancellationToken),
            Source12Index => Sources.Source12.SubscribeAsync(new SyncLatestWitness<T12, TResult>(this, Source12Bit, value => Value12 = new(value)), cancellationToken),
            Source13Index => Sources.Source13.SubscribeAsync(new SyncLatestWitness<T13, TResult>(this, Source13Bit, value => Value13 = new(value)), cancellationToken),
            _ => Sources.Source14.SubscribeAsync(new SyncLatestWitness<T14, TResult>(this, Source14Bit, value => Value14 = new(value)), cancellationToken)
        };

    /// <summary>
    /// Reads every source's latest value into a single snapshot. Returns <see langword="false"/>
    /// until every source has produced at least one value.
    /// </summary>
    /// <param name="values">When the method returns <see langword="true"/>, the snapshot.</param>
    /// <returns><see langword="true"/> when every source has produced a value; otherwise <see langword="false"/>.</returns>
    [SuppressMessage(
        "Maintainability",
        "SST1442:A function has too many direct branch points",
        Justification =
            "One short-circuited Optional check per source is the snapshot semantic; the branch count is inherent to the operator arity.")]
    private bool TryReadValues(
        out (T1 V1, T2 V2, T3 V3, T4 V4, T5 V5, T6 V6, T7 V7, T8 V8, T9 V9, T10 V10, T11 V11, T12 V12, T13 V13, T14 V14)
            values)
    {
        if (Value1.TryGetValue(out var value1)
            && Value2.TryGetValue(out var value2)
            && Value3.TryGetValue(out var value3)
            && Value4.TryGetValue(out var value4)
            && Value5.TryGetValue(out var value5)
            && Value6.TryGetValue(out var value6)
            && Value7.TryGetValue(out var value7)
            && Value8.TryGetValue(out var value8)
            && Value9.TryGetValue(out var value9)
            && Value10.TryGetValue(out var value10)
            && Value11.TryGetValue(out var value11)
            && Value12.TryGetValue(out var value12)
            && Value13.TryGetValue(out var value13)
            && Value14.TryGetValue(out var value14))
        {
            values = (value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11, value12,
                value13, value14);
            return true;
        }

        values = default;
        return false;
    }
}
