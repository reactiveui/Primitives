// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

#pragma warning disable S109 // Scenario tests intentionally use domain-sized constants.
#pragma warning disable S1192 // Repeated expected strings keep assertions readable in scenario tests.
#pragma warning disable CA1861 // Inline expected arrays are test constants and are never mutated.

/// <summary>
/// Deterministic real-world scenario tests covering mixed data shapes and update rates.
/// </summary>
public sealed class RealWorldReactiveScenarioTests
{
    /// <summary>
    /// Verifies that nullable view-model updates project into read-only state and propagate terminal events.
    /// </summary>
    [Test]
    public void ViewModelStateProjectsNullableRecordUpdatesAndLateSubscribers()
    {
        var source = new Signal<SearchUpdate?>();
        using var state = source
            .KeepNotNull()
            .ToReadOnlyState(new SearchState(string.Empty, 0, false), update => new SearchState(update.Query, update.Count, update.OptionalText is not null));

        var firstValues = new List<SearchState>();
        var completions = 0;
        using var first = state.Subscribe(firstValues.Add, _ => { }, () => completions++);

        source.OnNext(null);
        source.OnNext(new SearchUpdate("rx", 2, null));
        source.OnNext(new SearchUpdate("rx primitives", 5, "cached"));

        var lateValues = new List<SearchState>();
        using var late = state.Subscribe(lateValues.Add);
        source.OnCompleted();

        var expectedFirst = new[]
        {
            new SearchState(string.Empty, 0, false),
            new SearchState("rx", 2, false),
            new SearchState("rx primitives", 5, true),
        };
        var expectedLate = new[] { new SearchState("rx primitives", 5, true) };

        Assert.Equal(expectedFirst, firstValues);
        Assert.Equal(expectedLate, lateValues);
        Assert.Equal(1, completions);
        Assert.Equal(new SearchState("rx primitives", 5, true), state.Value);
    }

    /// <summary>
    /// Verifies default-if-empty behavior over hot sources for empty, non-empty, error, and observer-guard branches.
    /// </summary>
    [Test]
    public void DefaultIfEmptyCoversHotSourceEmptyNonEmptyErrorAndObserverGuard()
    {
        var emptySource = new Signal<string?>();
        var empty = new RecordingObserver<string?>();
        emptySource.DefaultIfEmpty("fallback").Subscribe(empty);
        emptySource.OnCompleted();

        var nonEmptySource = new Signal<string?>();
        var nonEmpty = new RecordingObserver<string?>();
        nonEmptySource.DefaultIfEmpty("fallback").Subscribe(nonEmpty);
        nonEmptySource.OnNext(null);
        nonEmptySource.OnNext("actual");
        nonEmptySource.OnCompleted();

        var errorSource = new Signal<string?>();
        var errors = new RecordingObserver<string?>();
        errorSource.DefaultIfEmpty("fallback").Subscribe(errors);
        errorSource.OnError(new InvalidOperationException("broken"));

        Assert.Throws<ArgumentNullException>(() => emptySource.DefaultIfEmpty("x").Subscribe(null!));
        Assert.Equal(new[] { "fallback" }, empty.Values);
        Assert.Equal(1, empty.Completed);
        Assert.Equal(new string?[] { null, "actual" }, nonEmpty.Values);
        Assert.Equal(1, nonEmpty.Completed);
        Assert.Equal(new[] { "broken" }, errors.Errors);
    }

    /// <summary>
    /// Verifies burst telemetry buffering, high-throughput terminal aggregation, and subscriber churn.
    /// </summary>
    [Test]
    public void TelemetryBurstBuffersTerminalCountsAndSubscriberChurnAreDeterministic()
    {
        var source = new Signal<Metric>();
        var retained = new List<Metric>();
        var churned = new List<Metric>();
        var buffers = new List<IList<Metric>>();
        using var retainedSubscription = source.Subscribe(retained.Add);
        var churnedSubscription = source.Subscribe(churned.Add);
        using var bufferedSubscription = source.Buffer(32).Subscribe(buffers.Add);

        for (var i = 0; i < 64; i++)
        {
            source.OnNext(new Metric(i, i * 0.5, i % 10 == 0));
        }

        churnedSubscription.Dispose();

        for (var i = 64; i < 70; i++)
        {
            source.OnNext(new Metric(i, i * 0.5, i % 10 == 0));
        }

        source.OnCompleted();

        var terminalSource = Signal.FromEnumerable(retained);
        var count = terminalSource.Count(metric => metric.IsCritical);
        var anyHigh = terminalSource.Any(metric => metric.Value > 30);
        var allNonNegative = terminalSource.All(metric => metric.Sequence >= 0);
        var contains = terminalSource.Contains(new Metric(20, 10, true));

        Assert.Equal(70, retained.Count);
        Assert.Equal(64, churned.Count);
        Assert.Equal(3, buffers.Count);
        Assert.Equal(32, buffers[0].Count);
        Assert.Equal(32, buffers[1].Count);
        Assert.Equal(6, buffers[2].Count);
        Assert.Equal(new[] { 7 }, Capture(count));
        Assert.Equal(new[] { true }, Capture(anyHigh));
        Assert.Equal(new[] { true }, Capture(allNonNegative));
        Assert.Equal(new[] { true }, Capture(contains));
        Assert.True(retainedSubscription is not null);
    }

    /// <summary>
    /// Verifies event-pattern bridges preserve sender/arguments and detach handlers on disposal.
    /// </summary>
    [Test]
    public void EventPatternBridgePreservesSenderArgumentsAndDisposesHandler()
    {
        var button = new FakeButton();
        var events = new List<EventPattern<FakeClickEventArgs>>();
        using (Signal.FromEventPattern<FakeClickEventArgs>(handler => button.Clicked += handler, handler => button.Clicked -= handler)
            .Subscribe(events.Add))
        {
            button.Raise("open");
            button.Raise("save");
        }

        button.Raise("ignored");

        Assert.Equal(2, events.Count);
        Assert.Same(button, events[0].Sender!);
        Assert.Equal("open", events[0].EventArgs.Command);
        Assert.Equal("save", events[1].EventArgs.Command);
        Assert.Equal(0, button.SubscriberCount);
        Assert.Equal("ReactiveUI.Primitives.Tests.RealWorldReactiveScenarioTests+FakeButton: ReactiveUI.Primitives.Tests.RealWorldReactiveScenarioTests+FakeClickEventArgs", events[0].ToString());
    }

    /// <summary>
    /// Verifies collection and async terminal operators with reference, record, and nullable values.
    /// </summary>
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
        var firstDefault = await Signal.None<Contact>().FirstOrDefaultAsync(new Contact("empty", null));
        var last = await source.LastAsync();
        var anyNullLastName = await source.AnyAsync(contact => contact.LastName is null);
        var countWithLastName = await source.CountAsync(contact => contact.LastName is not null);

        Assert.Equal<Contact>(contacts, collectedArray);
        Assert.Equal<Contact>(contacts, collectedList);
        Assert.Equal(new Contact("empty", null), firstDefault);
        Assert.Equal(new Contact("Katherine", "Johnson"), last);
        Assert.True(anyNullLastName);
        Assert.Equal(2, countWithLastName);
    }

    /// <summary>
    /// Captures values emitted by a synchronous signal.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="source">The source to observe.</param>
    /// <returns>The captured values.</returns>
    private static List<T> Capture<T>(IObservable<T> source)
    {
        var values = new List<T>();
        source.Subscribe(values.Add);
        return values;
    }

    /// <summary>
    /// Telemetry metric value type used by high-throughput scenarios.
    /// </summary>
    /// <param name="Sequence">The sequence number.</param>
    /// <param name="Value">The metric value.</param>
    /// <param name="IsCritical">A value indicating whether the metric is critical.</param>
    private readonly record struct Metric(long Sequence, double Value, bool IsCritical);

    /// <summary>
    /// Search update input record with nullable data.
    /// </summary>
    /// <param name="Query">The search query.</param>
    /// <param name="Count">The result count.</param>
    /// <param name="OptionalText">Optional metadata.</param>
    private sealed record SearchUpdate(string Query, int Count, string? OptionalText);

    /// <summary>
    /// Projected state record used by view-model state scenarios.
    /// </summary>
    /// <param name="Query">The current query.</param>
    /// <param name="Count">The result count.</param>
    /// <param name="HasOptionalText">A value indicating whether optional text is present.</param>
    private sealed record SearchState(string Query, int Count, bool HasOptionalText);

    /// <summary>
    /// Contact reference record with nullable fields.
    /// </summary>
    /// <param name="FirstName">The first name.</param>
    /// <param name="LastName">The optional last name.</param>
    private sealed record Contact(string FirstName, string? LastName);

    /// <summary>
    /// Event arguments for fake click events.
    /// </summary>
    private sealed class FakeClickEventArgs : EventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FakeClickEventArgs"/> class.
        /// </summary>
        /// <param name="command">The command associated with the click.</param>
        public FakeClickEventArgs(string command) => Command = command;

        /// <summary>
        /// Gets the command associated with the click.
        /// </summary>
        public string Command { get; }
    }

    /// <summary>
    /// Fake event source used by event-pattern bridge scenarios.
    /// </summary>
    private sealed class FakeButton
    {
        /// <summary>
        /// Raised when the fake button is clicked.
        /// </summary>
        public event EventHandler<FakeClickEventArgs>? Clicked;

        /// <summary>
        /// Gets the current subscriber count.
        /// </summary>
        public int SubscriberCount => Clicked?.GetInvocationList().Length ?? 0;

        /// <summary>
        /// Raises the fake click event.
        /// </summary>
        /// <param name="command">The click command.</param>
        public void Raise(string command) => Clicked?.Invoke(this, new FakeClickEventArgs(command));
    }

    /// <summary>
    /// Observer that records values, errors, and completions.
    /// </summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>
        /// Gets the recorded values.
        /// </summary>
        public List<T> Values { get; } = [];

        /// <summary>
        /// Gets the recorded error messages.
        /// </summary>
        public List<string> Errors { get; } = [];

        /// <summary>
        /// Gets the number of completion notifications.
        /// </summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error.Message);

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
