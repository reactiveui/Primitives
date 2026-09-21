// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the <c>GroupBy</c> and <c>GroupByUntil</c> overloads.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>The initial capacity handed to the capacity overloads.</summary>
    private const int InitialCapacity = 4;

    /// <summary>The log of a source that emits "a1", "b1" and "A2" and completes, grouped case-sensitively by the first character.</summary>
    private const string CaseSensitiveLog = "open ga ga:a1 open gb gb:b1 open gA gA:A2 ga:done gb:done gA:done outer:done";

    /// <summary>The case-sensitive log with each value projected to upper case.</summary>
    private const string CaseSensitiveUpperLog = "open ga ga:A1 open gb gb:B1 open gA gA:A2 ga:done gb:done gA:done outer:done";

    /// <summary>The log of the same source grouped case-insensitively.</summary>
    private const string CaseInsensitiveLog = "open ga ga:a1 open gb gb:b1 ga:A2 ga:done gb:done outer:done";

    /// <summary>The case-insensitive log with each value projected to upper case.</summary>
    private const string CaseInsensitiveUpperLog = "open ga ga:A1 open gb gb:B1 ga:A2 ga:done gb:done outer:done";

    /// <summary>Grouping by key delivers a group per key.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_KeySelector_DeliversAGroupPerKey() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1]))).IsEqualTo(CaseSensitiveLog);

    /// <summary>Grouping by key with a comparer lets the comparer decide which values share a group.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_KeySelectorAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1], StringComparer.OrdinalIgnoreCase))).IsEqualTo(CaseInsensitiveLog);

    /// <summary>Grouping by key with a capacity delivers a group per key.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_KeySelectorAndCapacity_DeliversAGroupPerKey() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1], InitialCapacity))).IsEqualTo(CaseSensitiveLog);

    /// <summary>Grouping by key with a capacity and comparer uses the comparer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_KeySelectorCapacityAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1], InitialCapacity, StringComparer.OrdinalIgnoreCase))).IsEqualTo(CaseInsensitiveLog);

    /// <summary>Grouping with an element selector projects each value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_ElementSelector_ProjectsEachValue() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1], static v => v.ToUpperInvariant()))).IsEqualTo(CaseSensitiveUpperLog);

    /// <summary>Grouping with an element selector and comparer uses the comparer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_ElementSelectorAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1], static v => v.ToUpperInvariant(), StringComparer.OrdinalIgnoreCase)))
            .IsEqualTo(CaseInsensitiveUpperLog);

    /// <summary>Grouping with an element selector and capacity projects each value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_ElementSelectorAndCapacity_ProjectsEachValue() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1], static v => v.ToUpperInvariant(), InitialCapacity)))
            .IsEqualTo(CaseSensitiveUpperLog);

    /// <summary>Grouping with an element selector, capacity and comparer uses the comparer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_ElementSelectorCapacityAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupBy(static v => v[..1], static v => v.ToUpperInvariant(), InitialCapacity, StringComparer.OrdinalIgnoreCase)))
            .IsEqualTo(CaseInsensitiveUpperLog);

    /// <summary>A null source, selector or negative capacity is rejected by every <c>GroupBy</c> overload.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupBy_InvalidArguments_Throw()
    {
        var source = Signal.None<string>();
        Func<string, string> key = static v => v;
        Func<string, int> element = static v => v.Length;

        await Assert.That(() => ((IObservable<string>)null!).GroupBy(key)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.GroupBy((Func<string, string>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.GroupBy(key, (Func<string, int>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.GroupBy(key, -1)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.GroupBy(key, -1, StringComparer.Ordinal)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.GroupBy(key, element, -1)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.GroupBy(key, element, -1, StringComparer.Ordinal)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Grouping until a duration delivers a group per key.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_DurationSelector_DeliversAGroupPerKey() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(static v => v[..1], static _ => Signal.Silent<int>()))).IsEqualTo(CaseSensitiveLog);

    /// <summary>Grouping until a duration with a comparer uses the comparer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_DurationSelectorAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(static v => v[..1], static _ => Signal.Silent<int>(), StringComparer.OrdinalIgnoreCase)))
            .IsEqualTo(CaseInsensitiveLog);

    /// <summary>Grouping until a duration with a capacity delivers a group per key.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_DurationSelectorAndCapacity_DeliversAGroupPerKey() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(static v => v[..1], static _ => Signal.Silent<int>(), InitialCapacity)))
            .IsEqualTo(CaseSensitiveLog);

    /// <summary>Grouping until a duration with a capacity and comparer uses the comparer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_DurationSelectorCapacityAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(static v => v[..1], static _ => Signal.Silent<int>(), InitialCapacity, StringComparer.OrdinalIgnoreCase)))
            .IsEqualTo(CaseInsensitiveLog);

    /// <summary>Grouping until a duration with an element selector projects each value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_ElementSelector_ProjectsEachValue() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(static v => v[..1], static v => v.ToUpperInvariant(), static _ => Signal.Silent<int>())))
            .IsEqualTo(CaseSensitiveUpperLog);

    /// <summary>Grouping until a duration with an element selector and comparer uses the comparer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_ElementSelectorAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(static v => v[..1], static v => v.ToUpperInvariant(), static _ => Signal.Silent<int>(), StringComparer.OrdinalIgnoreCase)))
            .IsEqualTo(CaseInsensitiveUpperLog);

    /// <summary>Grouping until a duration with an element selector and capacity projects each value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_ElementSelectorAndCapacity_ProjectsEachValue() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(static v => v[..1], static v => v.ToUpperInvariant(), static _ => Signal.Silent<int>(), InitialCapacity)))
            .IsEqualTo(CaseSensitiveUpperLog);

    /// <summary>Grouping until a duration with an element selector, capacity and comparer uses the comparer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_ElementSelectorCapacityAndComparer_UsesTheComparer() =>
        await Assert.That(GroupLog(static s => s.GroupByUntil(
            static v => v[..1],
            static v => v.ToUpperInvariant(),
            static _ => Signal.Silent<int>(),
            InitialCapacity,
            StringComparer.OrdinalIgnoreCase))).IsEqualTo(CaseInsensitiveUpperLog);

    /// <summary>A null source, selector or negative capacity is rejected by every <c>GroupByUntil</c> overload.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GroupByUntil_InvalidArguments_Throw()
    {
        var source = Signal.None<string>();
        Func<string, string> key = static v => v;
        Func<string, int> element = static v => v.Length;
        Func<GroupedSignal<string, string>, IObservable<int>> duration = static _ => Signal.Silent<int>();
        Func<GroupedSignal<string, int>, IObservable<int>> elementDuration = static _ => Signal.Silent<int>();

        await Assert.That(() => ((IObservable<string>)null!).GroupByUntil(key, duration)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.GroupByUntil(key, (Func<GroupedSignal<string, string>, IObservable<int>>)null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => source.GroupByUntil(key, duration, -1)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.GroupByUntil(key, duration, -1, StringComparer.Ordinal)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.GroupByUntil(key, element, elementDuration, -1)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => source.GroupByUntil(key, element, elementDuration, -1, StringComparer.Ordinal)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Subscribes a group recorder to an operator over a subject that emits "a1", "b1" and "A2" and completes.</summary>
    /// <typeparam name="TElement">The value type of each group.</typeparam>
    /// <param name="build">Applies the operator under test.</param>
    /// <returns>The recorded log.</returns>
    private static string GroupLog<TElement>(Func<Signal<string>, IObservable<GroupedSignal<string, TElement>>> build)
    {
        Signal<string> source = new();
        GroupRecordingWitness<string, TElement> observer = new();
        using var subscription = build(source).Subscribe(observer);
        PushThenComplete(source, "a1", "b1", "A2");
        return observer.Text;
    }
}
