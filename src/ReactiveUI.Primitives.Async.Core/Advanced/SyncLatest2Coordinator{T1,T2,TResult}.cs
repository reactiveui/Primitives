// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates subscriptions and latest-value emission for the arity-2 <c>SyncLatest</c> operator.</summary>
/// <typeparam name="T1">Element type of source 1.</typeparam>
/// <typeparam name="T2">Element type of source 2.</typeparam>
/// <typeparam name="TResult">The projected element type.</typeparam>
public sealed class SyncLatest2Coordinator<T1, T2, TResult> : SyncLatestCoordinatorBase<TResult>
{
    /// <summary>Bit owned by source 1 inside the lifecycle's completion bitmask.</summary>
    private const int Source1Bit = 1 << 0;

    /// <summary>Bit owned by source 2 inside the lifecycle's completion bitmask.</summary>
    private const int Source2Bit = 1 << 1;

    /// <summary>Initializes a new instance of the <see cref="SyncLatest2Coordinator{T1, T2, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    public SyncLatest2Coordinator(
        IObserverAsync<TResult> observer,
        SyncLatest2State<T1, T2> sources,
        Func<T1, T2, TResult> selector)
        : base(observer, sourceCount: 2)
    {
        Sources = sources;
        Selector = selector;
        Observer1 = new(this, Source1Bit, value => Value1 = new(value));
        Observer2 = new(this, Source2Bit, value => Value2 = new(value));
    }

    /// <summary>Gets the bundled source observables.</summary>
    private SyncLatest2State<T1, T2> Sources { get; }

    /// <summary>Gets the selector that projects the latest values.</summary>
    private Func<T1, T2, TResult> Selector { get; }

    /// <summary>Gets the indexed observer for source 1.</summary>
    private SyncLatestWitness<T1, TResult> Observer1 { get; }

    /// <summary>Gets the indexed observer for source 2.</summary>
    private SyncLatestWitness<T2, TResult> Observer2 { get; }

    /// <summary>Gets or sets the latest value from source 1.</summary>
    private Optional<T1> Value1 { get; set; } = Optional<T1>.Empty;

    /// <summary>Gets or sets the latest value from source 2.</summary>
    private Optional<T2> Value2 { get; set; } = Optional<T2>.Empty;

    /// <inheritdoc/>
    internal override ValueTask EmitLatestAsync()
    {
        if (!TryReadValues(out var values))
        {
            return default;
        }

        var projected = Selector(
            values.V1,
            values.V2);
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
            _ => Sources.Source2.SubscribeAsync(Observer2, cancellationToken),
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
    private bool TryReadValues(out (T1 V1, T2 V2) values)
    {
        if (Value1.TryGetValue(out var value1)
                    && Value2.TryGetValue(out var value2))
        {
            values = (value1, value2);
            return true;
        }

        values = default;
        return false;
    }
}
