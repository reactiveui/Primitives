// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates subscriptions and latest-value emission for the arity-8 <c>SyncLatest</c> operator.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <typeparam name="T3">Element type of source 3.</typeparam>
/// <typeparam name="T4">Element type of source 4.</typeparam>
/// <typeparam name="T5">Element type of source 5.</typeparam>
/// <typeparam name="T6">Element type of source 6.</typeparam>
/// <typeparam name="T7">Element type of source 7.</typeparam>
/// <typeparam name="T8">Element type of source 8.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
public sealed class SyncLatest8Coordinator<T1, T2, T3, T4, T5, T6, T7, T8, TResult> : SyncLatestCoordinatorBase<TResult>
{
    /// <summary>Bit owned by source 1 inside the lifecycle's completion bitmask.</summary>
    private const int Source1Bit = 1 << 0;

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

    /// <summary>Initializes a new instance of the <see cref="SyncLatest8Coordinator{T1, T2, T3, T4, T5, T6, T7, T8, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    public SyncLatest8Coordinator(
        IObserverAsync<TResult> observer,
        SyncLatest8State<T1, T2, T3, T4, T5, T6, T7, T8> sources,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> selector)
        : base(observer, sourceCount: 8)
    {
        Sources = sources;
        Selector = selector;
        Observer1 = new(this, Source1Bit, value => Value1 = new(value));
        Observer2 = new(this, Source2Bit, value => Value2 = new(value));
        Observer3 = new(this, Source3Bit, value => Value3 = new(value));
        Observer4 = new(this, Source4Bit, value => Value4 = new(value));
        Observer5 = new(this, Source5Bit, value => Value5 = new(value));
        Observer6 = new(this, Source6Bit, value => Value6 = new(value));
        Observer7 = new(this, Source7Bit, value => Value7 = new(value));
        Observer8 = new(this, Source8Bit, value => Value8 = new(value));
    }

    /// <summary>Gets the bundled source observables.</summary>
    private SyncLatest8State<T1, T2, T3, T4, T5, T6, T7, T8> Sources { get; }

    /// <summary>Gets the selector that projects the latest values.</summary>
    private Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> Selector { get; }

    /// <summary>Gets the indexed observer for source 1.</summary>
    private SyncLatestWitness<T1, TResult> Observer1 { get; }

    /// <summary>Gets the indexed observer for source 2.</summary>
    private SyncLatestWitness<T2, TResult> Observer2 { get; }

    /// <summary>Gets the indexed observer for source 3.</summary>
    private SyncLatestWitness<T3, TResult> Observer3 { get; }

    /// <summary>Gets the indexed observer for source 4.</summary>
    private SyncLatestWitness<T4, TResult> Observer4 { get; }

    /// <summary>Gets the indexed observer for source 5.</summary>
    private SyncLatestWitness<T5, TResult> Observer5 { get; }

    /// <summary>Gets the indexed observer for source 6.</summary>
    private SyncLatestWitness<T6, TResult> Observer6 { get; }

    /// <summary>Gets the indexed observer for source 7.</summary>
    private SyncLatestWitness<T7, TResult> Observer7 { get; }

    /// <summary>Gets the indexed observer for source 8.</summary>
    private SyncLatestWitness<T8, TResult> Observer8 { get; }

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
            values.V8);
        return Lifecycle.EmitDownstreamAsync(projected);
    }

    /// <inheritdoc/>
    [SuppressMessage(
        "Minor Code Smell",
        "S109:Magic numbers should not be used",
        Justification = "Switch dispatches on the 0..N-1 source index; naming each numeric arm would just rename the obvious.")]
    [SuppressMessage(
        "Major Code Smell",
        "S1541:Methods and properties should not be too complex",
        Justification = "Switch arm per source; the high arm count is the dispatch surface.")]
    protected override ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken) =>
        index switch
        {
            0 => Sources.Source1.SubscribeAsync(Observer1, cancellationToken),
            1 => Sources.Source2.SubscribeAsync(Observer2, cancellationToken),
            2 => Sources.Source3.SubscribeAsync(Observer3, cancellationToken),
            3 => Sources.Source4.SubscribeAsync(Observer4, cancellationToken),
            4 => Sources.Source5.SubscribeAsync(Observer5, cancellationToken),
            5 => Sources.Source6.SubscribeAsync(Observer6, cancellationToken),
            6 => Sources.Source7.SubscribeAsync(Observer7, cancellationToken),
            _ => Sources.Source8.SubscribeAsync(Observer8, cancellationToken),
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
    private bool TryReadValues(out (T1 V1, T2 V2, T3 V3, T4 V4, T5 V5, T6 V6, T7 V7, T8 V8) values)
    {
        if (Value1.TryGetValue(out var value1)
                    && Value2.TryGetValue(out var value2)
                    && Value3.TryGetValue(out var value3)
                    && Value4.TryGetValue(out var value4)
                    && Value5.TryGetValue(out var value5)
                    && Value6.TryGetValue(out var value6)
                    && Value7.TryGetValue(out var value7)
                    && Value8.TryGetValue(out var value8))
        {
            values = (value1, value2, value3, value4, value5, value6, value7, value8);
            return true;
        }

        values = default;
        return false;
    }
}
