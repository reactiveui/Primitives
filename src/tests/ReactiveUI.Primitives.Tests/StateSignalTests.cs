// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="StateSignal{T}"/> read-only projection contracts.</summary>
public class StateSignalTests
{
    /// <summary>The first expected value.</summary>
    private const int First = 1;

    /// <summary>The second expected value.</summary>
    private const int Second = 2;

    /// <summary>The search query used by the view-model projection scenario.</summary>
    private const string SearchQuery = "rx primitives";

    /// <summary>Initial state value used by projection tests.</summary>
    private const int InitialStateValue = 10;

    /// <summary>Updated state value used by projection tests.</summary>
    private const int UpdatedStateValue = 11;

    /// <summary>A third state value, published after a projection subscription has been detached.</summary>
    private const int FinalStateValue = 12;

    /// <summary>Expected mutable state values.</summary>
    private static readonly int[] ExpectedStateValues = [InitialStateValue, UpdatedStateValue, UpdatedStateValue];

    /// <summary>Expected projected read-only state values.</summary>
    private static readonly string[] ExpectedReadOnlyValues = ["v:10", "v:11", "v:11"];

    /// <summary>Covers read-only projection error argument validation.</summary>
    [Test]
    public void ReadOnlyStateProjectionValidatesError()
    {
        StateSignal<int> source = new(First);
        using var projection = source.ToReadOnlyState(static value => value);
        _ = Assert.Throws<ArgumentNullException>(() => projection.OnError(null!));
    }

    /// <summary>Covers read-only projection selector errors forwarded to current and late subscribers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReadOnlyStateProjectionForwardsSelectorErrorToLateSubscribers()
    {
        StateSignal<int> source = new(First);
        using var projection = source.ToReadOnlyState(static value =>
            value == Second ? throw new InvalidOperationException("selector") : value);
        Recorder<int> projected = new();
        _ = projection.Subscribe(projected);
        source.Value = Second;
        await Assert.That(projected.Errors.Count).IsEqualTo(1);
        await Assert.That(projected.Errors[0].Message).IsEqualTo("selector");
        Recorder<int> lateProjected = new();
        _ = projection.Subscribe(lateProjected);
        await Assert.That(lateProjected.Errors.Count).IsEqualTo(1);
        await Assert.That(lateProjected.Errors[0]).IsSameReferenceAs(projected.Errors[0]);
    }

    /// <summary>Verifies that nullable view-model updates project into read-only state and propagate terminal events.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ViewModelStateProjectsNullableRecordUpdatesAndLateSubscribers()
    {
        const int FirstCount = 2;
        const int SecondCount = 5;
        Signal<SearchUpdate?> source = new();
        using var state = source.KeepNotNull().ToReadOnlyState(
            new(string.Empty, 0, false),
            static update => new SearchState(update.Query, update.Count, update.OptionalText is not null));
        List<SearchState> firstValues = [];
        var completions = 0;
        using var first = state.Subscribe(firstValues.Add, static _ => { }, () => completions++);
        source.OnNext(null);
        source.OnNext(new("rx", FirstCount, null));
        source.OnNext(new(SearchQuery, SecondCount, "cached"));
        List<SearchState> lateValues = [];
        using var late = state.Subscribe(lateValues.Add);
        source.OnCompleted();
        SearchState[] expectedFirst =
        [
            new(string.Empty, 0, false), new("rx", FirstCount, false),
            new(SearchQuery, SecondCount, true)
        ];
        SearchState[] expectedLate = [new(SearchQuery, SecondCount, true)];
        await Assert.That(firstValues.SequenceEqual(expectedFirst)).IsTrue();
        await Assert.That(lateValues.SequenceEqual(expectedLate)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(state.Value).IsEqualTo(new(SearchQuery, SecondCount, true));
    }

    /// <summary>Verifies mutable state exposes latest values and read-only projected values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulSignalsExposeLatestValuesAndReadOnlyProjections()
    {
        StateSignal<int> state = new(InitialStateValue);
        List<int> values = [];
        List<string> readonlyValues = [];
        _ = state.Changed.Subscribe(values.Add);
        using var readOnly = state.ToReadOnlyState(static value => $"v:{value}");
        _ = readOnly.Changed.Subscribe(readonlyValues.Add);
        state.Value = UpdatedStateValue;
        state.Refresh();
        await Assert.That(state.Value).IsEqualTo(UpdatedStateValue);
        await Assert.That(readOnly.Value).IsEqualTo("v:11");
        await Assert.That(values.SequenceEqual(ExpectedStateValues)).IsTrue();
        await Assert.That(readonlyValues.SequenceEqual(ExpectedReadOnlyValues)).IsTrue();
    }

    /// <summary>Verifies mutable state reports observer state, value availability, terminal errors, and disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulSignalsReportObserverValueErrorAndDisposalState()
    {
        StateSignal<int> state = new(InitialStateValue);
        await Assert.That(state.HasObservers).IsFalse();
        await Assert.That(state.IsDisposed).IsFalse();
        await Assert.That(state.TryGetValue(out var current)).IsTrue();
        await Assert.That(current).IsEqualTo(InitialStateValue);

        Recorder<int> observer = new();
        var subscription = state.Subscribe(observer);
        await Assert.That(state.HasObservers).IsTrue();
        state.OnNext(UpdatedStateValue);
        await Assert.That(observer.Values.SequenceEqual([InitialStateValue, UpdatedStateValue])).IsTrue();

        subscription.Dispose();
        await Assert.That(state.HasObservers).IsFalse();

        var error = new InvalidOperationException("state-error");
        state.OnError(error);
        Recorder<int> lateObserver = new();
        _ = state.Subscribe(lateObserver);
        await Assert.That(lateObserver.Errors.Single()).IsSameReferenceAs(error);

        state.Dispose();
        await Assert.That(state.IsDisposed).IsTrue();
    }

    /// <summary>
    /// A state signal that has already completed never replays its value on subscription, so the projection has
    /// no value to seed from its source. It must fall back to projecting the source's current value directly,
    /// and then replay that value plus the completion to anyone who subscribes later.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReadOnlyStateProjectionSeedsItselfFromASourceThatHasAlreadyCompleted()
    {
        StateSignal<int> source = new(InitialStateValue);
        source.OnCompleted();

        using var projection = source.ToReadOnlyState(static value => $"v:{value}");
        Recorder<string> late = new();
        _ = projection.Subscribe(late);

        await Assert.That(projection.Value).IsEqualTo("v:10");
        await Assert.That(late.Values.SequenceEqual(["v:10"])).IsTrue();
        await Assert.That(late.Completed).IsEqualTo(1);
        await Assert.That(late.Errors.Count).IsEqualTo(0);
    }

    /// <summary>
    /// Once the projection has completed, a source that keeps notifying must not be able to move the projected
    /// value, complete the subscribers twice, or turn a completed projection into a faulted one.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReadOnlyStateProjectionIgnoresSourceNotificationsAfterItsTerminal()
    {
        StateSignal<int> source = new(InitialStateValue);
        using var projection = source.ToReadOnlyState(static value => $"v:{value}");
        Recorder<string> observer = new();
        _ = projection.Subscribe(observer);

        // The projection is its own observer of the source, so driving it directly is what a source that
        // keeps notifying after the terminal looks like from the projection's side.
        projection.OnCompleted();
        projection.OnCompleted();
        projection.OnNext(UpdatedStateValue);
        projection.OnError(new InvalidOperationException("late"));

        await Assert.That(projection.Value).IsEqualTo("v:10");
        await Assert.That(observer.Values.SequenceEqual(["v:10"])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
    }

    /// <summary>
    /// Disposing a projection subscription detaches that observer and only that observer; disposing the same
    /// handle again is a no-op rather than a second detach.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReadOnlyStateProjectionStopsFeedingADisposedSubscription()
    {
        StateSignal<int> source = new(InitialStateValue);
        using var projection = source.ToReadOnlyState(static value => $"v:{value}");
        Recorder<string> detached = new();
        Recorder<string> attached = new();
        var subscription = projection.Subscribe(detached);
        _ = projection.Subscribe(attached);

        source.Value = UpdatedStateValue;
        subscription.Dispose();
        subscription.Dispose();
        source.Value = FinalStateValue;

        await Assert.That(detached.Values.SequenceEqual(["v:10", "v:11"])).IsTrue();
        await Assert.That(attached.Values.SequenceEqual(["v:10", "v:11", "v:12"])).IsTrue();
    }

    /// <summary>
    /// A disposed projection has thrown its state away, so reading or subscribing to it must fail loudly rather
    /// than hand back a stale value. Disposing twice must still be safe.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposedReadOnlyStateProjectionRejectsReadsAndSubscriptions()
    {
        StateSignal<int> source = new(InitialStateValue);
        var projection = source.ToReadOnlyState(static value => $"v:{value}");

        projection.Dispose();
        projection.Dispose();

        _ = Assert.Throws<ObjectDisposedException>(() => _ = projection.Value);
        _ = Assert.Throws<ObjectDisposedException>(() => projection.Subscribe(new Recorder<string>()));

        // The source outlives its projection and must remain usable.
        source.Value = UpdatedStateValue;
        await Assert.That(source.Value).IsEqualTo(UpdatedStateValue);
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>Search update input record with nullable data.</summary>
    /// <param name="Query">The search query.</param>
    /// <param name="Count">The result count.</param>
    /// <param name="OptionalText">Optional metadata.</param>
    private sealed record SearchUpdate(string Query, int Count, string? OptionalText);

    /// <summary>Projected state record used by view-model state scenarios.</summary>
    /// <param name="Query">The current query.</param>
    /// <param name="Count">The result count.</param>
    /// <param name="HasOptionalText">A value indicating whether optional text is present.</param>
    private sealed record SearchState(string Query, int Count, bool HasOptionalText);
}
