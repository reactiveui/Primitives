// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#pragma warning disable S103, S6966 // Coverage tests intentionally group branch-heavy scenarios.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Deterministic real-world scenario tests covering mixed data shapes and update rates.</summary>
public sealed class RealWorldReactiveScenarioTests
{
    /// <summary>A reused value of two for element indices and expected counts.</summary>
    private const int Two = 2;

    /// <summary>The fallback value emitted when a source is empty.</summary>
    private const string FallbackValue = "fallback";

    /// <summary>The search query used by the view-model projection scenario.</summary>
    private const string SearchQuery = "rx primitives";

    /// <summary>Expected fallback values emitted when a source is empty.</summary>
    private static readonly string[] FallbackValues = [FallbackValue];

    /// <summary>Expected error values surfaced by a broken source.</summary>
    private static readonly string[] BrokenErrors = ["broken"];

    /// <summary>Expected single-element true result for terminal predicate captures.</summary>
    private static readonly bool[] TrueResult = [true];

    /// <summary>Verifies that nullable view-model updates project into read-only state and propagate terminal events.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ViewModelStateProjectsNullableRecordUpdatesAndLateSubscribers()
    {
        const int FirstCount = 2;
        const int SecondCount = 5;
        var source = new Signal<SearchUpdate?>();
        using var state = source.KeepNotNull().ToReadOnlyState(new(string.Empty, 0, false), update => new SearchState(update.Query, update.Count, update.OptionalText is not null));
        var firstValues = new List<SearchState>();
        var completions = 0;
        using var first = state.Subscribe(
            firstValues.Add,
            _ =>
        {
        },
            () => completions++);
        source.OnNext(null);
        source.OnNext(new("rx", FirstCount, null));
        source.OnNext(new(SearchQuery, SecondCount, "cached"));
        var lateValues = new List<SearchState>();
        using var late = state.Subscribe(lateValues.Add);
        source.OnCompleted();
        var expectedFirst = new[]
        {
            new SearchState(string.Empty, 0, false),
            new SearchState("rx", FirstCount, false),
            new SearchState(SearchQuery, SecondCount, true),
        };
        var expectedLate = new[]
        {
            new SearchState(SearchQuery, SecondCount, true)
        };
        await Assert.That(firstValues.SequenceEqual(expectedFirst)).IsTrue();
        await Assert.That(lateValues.SequenceEqual(expectedLate)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(state.Value).IsEqualTo(new(SearchQuery, SecondCount, true));
    }

    /// <summary>Verifies default-if-empty behavior over hot sources for empty, non-empty, error, and observer-guard branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DefaultIfEmptyCoversHotSourceEmptyNonEmptyErrorAndObserverGuard()
    {
        var emptySource = new Signal<string?>();
        var empty = new RecordingWitness<string?>();
        emptySource.DefaultIfEmpty(FallbackValue).Subscribe(empty);
        emptySource.OnCompleted();
        var nonEmptySource = new Signal<string?>();
        var nonEmpty = new RecordingWitness<string?>();
        nonEmptySource.DefaultIfEmpty(FallbackValue).Subscribe(nonEmpty);
        nonEmptySource.OnNext(null);
        nonEmptySource.OnNext("actual");
        nonEmptySource.OnCompleted();
        var errorSource = new Signal<string?>();
        var errors = new RecordingWitness<string?>();
        errorSource.DefaultIfEmpty(FallbackValue).Subscribe(errors);
        errorSource.OnError(new InvalidOperationException("broken"));
        Assert.Throws<ArgumentNullException>(() => emptySource.DefaultIfEmpty("x").Subscribe(null!));
        await Assert.That(empty.Values.SequenceEqual(FallbackValues)).IsTrue();
        await Assert.That(empty.Completed).IsEqualTo(1);
        string?[] expectedNonEmpty = [null, "actual"];
        await Assert.That(nonEmpty.Values.SequenceEqual(expectedNonEmpty)).IsTrue();
        await Assert.That(nonEmpty.Completed).IsEqualTo(1);
        await Assert.That(errors.Errors.SequenceEqual(BrokenErrors)).IsTrue();
    }

    /// <summary>Verifies burst telemetry buffering, high-throughput terminal aggregation, and subscriber churn.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TelemetryBurstBuffersTerminalCountsAndSubscriberChurnAreDeterministic()
    {
        const int BufferSize = 32;
        const int FirstBurstCount = 64;
        const int TotalCount = 70;
        const double ValueScale = 0.5;
        const int CriticalModulo = 10;
        const double HighValueThreshold = 30;
        const int ExpectedBufferCount = 3;
        const int LastBufferCount = 6;
        const int ExpectedCriticalCount = 7;
        const long ContainsSequence = 20;
        const double ContainsValue = 10;
        var source = new Signal<Metric>();
        var retained = new List<Metric>();
        var churned = new List<Metric>();
        var buffers = new List<IList<Metric>>();
        using var retainedSubscription = source.Subscribe(retained.Add);
        var churnedSubscription = source.Subscribe(churned.Add);
        using var bufferedSubscription = source.Buffer(BufferSize).Subscribe(buffers.Add);
        for (var i = 0; i < FirstBurstCount; i++)
        {
            source.OnNext(new(i, i * ValueScale, i % CriticalModulo == 0));
        }

        churnedSubscription.Dispose();
        for (var i = FirstBurstCount; i < TotalCount; i++)
        {
            source.OnNext(new(i, i * ValueScale, i % CriticalModulo == 0));
        }

        source.OnCompleted();
        var terminalSource = Signal.FromEnumerable(retained);
        var count = terminalSource.Count(metric => metric.IsCritical);
        var anyHigh = terminalSource.Any(metric => metric.Value > HighValueThreshold);
        var allNonNegative = terminalSource.All(metric => metric.Sequence >= 0);
        var contains = terminalSource.Contains(new(ContainsSequence, ContainsValue, true));
        await Assert.That(retained.Count).IsEqualTo(TotalCount);
        await Assert.That(churned.Count).IsEqualTo(FirstBurstCount);
        await Assert.That(buffers.Count).IsEqualTo(ExpectedBufferCount);
        await Assert.That(buffers[0].Count).IsEqualTo(BufferSize);
        await Assert.That(buffers[1].Count).IsEqualTo(BufferSize);
        await Assert.That(buffers[Two].Count).IsEqualTo(LastBufferCount);
        int[] expectedCriticalCounts = [ExpectedCriticalCount];
        await Assert.That(Capture(count).SequenceEqual(expectedCriticalCounts)).IsTrue();
        await Assert.That(Capture(anyHigh).SequenceEqual(TrueResult)).IsTrue();
        await Assert.That(Capture(allNonNegative).SequenceEqual(TrueResult)).IsTrue();
        await Assert.That(Capture(contains).SequenceEqual(TrueResult)).IsTrue();
        await Assert.That(retainedSubscription is not null).IsTrue();
    }

    /// <summary>Verifies event-pattern bridges preserve sender/arguments and detach handlers on disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EventPatternBridgePreservesSenderArgumentsAndDisposesHandler()
    {
        var button = new FakeButton();
        var events = new List<EventPattern<FakeClickEventArgs>>();
        using (Signal.FromEventPattern<FakeClickEventArgs>(handler => button.Clicked += handler, handler => button.Clicked -= handler).Subscribe(events.Add))
        {
            button.Raise("open");
            button.Raise("save");
        }

        button.Raise("ignored");
        await Assert.That(events.Count).IsEqualTo(Two);
        await Assert.That(events[0].Sender!).IsSameReferenceAs(button);
        await Assert.That(events[0].EventArgs.Command).IsEqualTo("open");
        await Assert.That(events[1].EventArgs.Command).IsEqualTo("save");
        await Assert.That(button.SubscriberCount).IsEqualTo(0);
        await Assert.That(events[0].ToString()).IsEqualTo("ReactiveUI.Primitives.Tests.RealWorldReactiveScenarioTests+FakeButton: ReactiveUI.Primitives.Tests.RealWorldReactiveScenarioTests+FakeClickEventArgs");
    }

    /// <summary>Verifies collection and async terminal operators with reference, record, and nullable values.</summary>
    /// <returns>A task that completes when assertions finish.</returns>
    [Test]
    public async Task CollectionAndAsyncTerminalsHandleReferenceRecordAndNullableValues()
    {
        var contacts = new[]
        {
            new Contact("Ada", "Lovelace"),
            new Contact("Grace", null),
            new Contact("Katherine", "Johnson"),
        };
        var source = Signal.FromEnumerable(contacts);
        var collectedArray = await source.CollectArrayAsync();
        var collectedList = await source.CollectListAsync();
        var firstDefault = await Signal.None<Contact>().FirstOrDefaultAsync(new("empty", null));
        var last = await source.LastAsync();
        var anyNullLastName = await source.AnyAsync(contact => contact.LastName is null);
        var countWithLastName = await source.CountAsync(contact => contact.LastName is not null);
        await Assert.That(collectedArray.SequenceEqual(contacts)).IsTrue();
        await Assert.That(collectedList.SequenceEqual(contacts)).IsTrue();
        await Assert.That(firstDefault).IsEqualTo(new("empty", null));
        await Assert.That(last).IsEqualTo(new("Katherine", "Johnson"));
        await Assert.That(anyNullLastName).IsTrue();
        await Assert.That(countWithLastName).IsEqualTo(Two);
    }

    /// <summary>Captures values emitted by a synchronous signal.</summary>
    /// <typeparam name = "T">The observed value type.</typeparam>
    /// <param name = "source">The source to observe.</param>
    /// <returns>The captured values.</returns>
    private static List<T> Capture<T>(IObservable<T> source)
    {
        var values = new List<T>();
        source.Subscribe(values.Add);
        return values;
    }

    /// <summary>Telemetry metric value type used by high-throughput scenarios.</summary>
    /// <param name = "Sequence">The sequence number.</param>
    /// <param name = "Value">The metric value.</param>
    /// <param name = "IsCritical">A value indicating whether the metric is critical.</param>
    private readonly record struct Metric(long Sequence, double Value, bool IsCritical);

    /// <summary>Event arguments for fake click events.</summary>
    private sealed class FakeClickEventArgs : EventArgs
    {
        /// <summary>Initializes a new instance of the <see cref = "FakeClickEventArgs"/> class.</summary>
        /// <param name = "command">The command associated with the click.</param>
        public FakeClickEventArgs(string command) => Command = command;

        /// <summary>Gets the command associated with the click.</summary>
        public string Command { get; }
    }

    /// <summary>Fake event source used by event-pattern bridge scenarios.</summary>
    private sealed class FakeButton
    {
        /// <summary>Raised when the fake button is clicked.</summary>
        public event EventHandler<FakeClickEventArgs>? Clicked;

        /// <summary>Gets the current subscriber count.</summary>
        public int SubscriberCount => Clicked?.GetInvocationList().Length ?? 0;

        /// <summary>Raises the fake click event.</summary>
        /// <param name = "command">The click command.</param>
        public void Raise(string command) => Clicked?.Invoke(this, new(command));
    }

    /// <summary>Observer that records values, errors, and completions.</summary>
    /// <typeparam name = "T">The observed value type.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets the recorded values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the recorded error messages.</summary>
        public List<string> Errors { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error.Message);

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>Search update input record with nullable data.</summary>
    /// <param name = "Query">The search query.</param>
    /// <param name = "Count">The result count.</param>
    /// <param name = "OptionalText">Optional metadata.</param>
    private sealed record SearchUpdate(string Query, int Count, string? OptionalText);

    /// <summary>Projected state record used by view-model state scenarios.</summary>
    /// <param name = "Query">The current query.</param>
    /// <param name = "Count">The result count.</param>
    /// <param name = "HasOptionalText">A value indicating whether optional text is present.</param>
    private sealed record SearchState(string Query, int Count, bool HasOptionalText);

    /// <summary>Contact reference record with nullable fields.</summary>
    /// <param name = "FirstName">The first name.</param>
    /// <param name = "LastName">The optional last name.</param>
    private sealed record Contact(string FirstName, string? LastName);
}
