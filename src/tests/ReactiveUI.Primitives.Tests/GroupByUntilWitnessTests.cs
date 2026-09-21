// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that routes values into keyed groups that end with a duration signal.</summary>
public class GroupByUntilWitnessTests
{
    /// <summary>A duration signal that emits ends its group, and the next value with that key opens a new group.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DurationEmits_EndsTheGroupAndTheKeyOpensAFreshGroup()
    {
        Signal<string> source = new();
        List<Signal<int>> durations = [];
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, _ => NewDuration(durations));
        source.OnNext("a1");
        source.OnNext("b1");
        source.OnNext("a2");

        durations[0].OnNext(0);
        source.OnNext("a3");
        source.OnNext("b2");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 open gb gb:b1 ga:a2 ga:done open ga ga:a3 gb:b2");
        await Assert.That(durations[durations.Count - 1].HasObservers).IsTrue();
    }

    /// <summary>A duration signal that completes ends its group.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DurationCompletes_EndsTheGroup()
    {
        Signal<string> source = new();
        Signal<int> duration = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, _ => duration);
        source.OnNext("a1");

        duration.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 ga:done");
    }

    /// <summary>The duration selector runs before the group is delivered and sees the group's key.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DurationSelector_RunsBeforeTheGroupIsDelivered()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        List<string> log = [];
        using var subscription = Group(
            source,
            observer,
            group =>
            {
                log.Add($"selector {group.Key} after {observer.Log.Count} notifications");
                return Signal.Silent<int>();
            });

        source.OnNext("a1");

        await Assert.That(string.Join(' ', log)).IsEqualTo("selector a after 0 notifications");
        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1");
    }

    /// <summary>A duration that fires as it is subscribed ends the group before its first value arrives.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DurationFiresImmediately_EndsTheGroupBeforeItsFirstValue()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, static _ => Signal.Return(0));

        source.OnNext("a1");
        source.OnNext("a2");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:done open ga ga:done");
    }

    /// <summary>A duration error faults every open group and the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DurationFaults_FaultsEveryGroupAndTheOuterSequence()
    {
        Signal<string> source = new();
        List<Signal<int>> durations = [];
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, _ => NewDuration(durations));
        source.OnNext("a1");
        source.OnNext("b1");

        durations[1].OnError(new InvalidOperationException("duration"));

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 open gb gb:b1 ga:error duration gb:error duration outer:error duration");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>A duration selector failure faults the sequence before the group is delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DurationSelectorThrows_FaultsTheOuterSequenceWithoutDeliveringTheGroup()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, static IObservable<int> (_) => throw new InvalidOperationException("selector"));

        source.OnNext("a1");

        await Assert.That(observer.Text).IsEqualTo("outer:error selector");
    }

    /// <summary>A key selector failure faults the sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task KeySelectorThrows_FaultsTheOuterSequence()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = new GroupByUntilSignal<string, char, string, int>(
            source,
            static char (_) => throw new InvalidOperationException("key"),
            static v => v,
            static _ => Signal.Silent<int>(),
            null,
            0).Subscribe(observer);

        source.OnNext("a1");

        await Assert.That(observer.Text).IsEqualTo("outer:error key");
    }

    /// <summary>An element selector failure faults the sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ElementSelectorThrows_FaultsTheOuterSequence()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = new GroupByUntilSignal<string, char, string, int>(
            source,
            static v => v[0],
            static string (_) => throw new InvalidOperationException("element"),
            static _ => Signal.Silent<int>(),
            null,
            0).Subscribe(observer);

        source.OnNext("a1");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:error element outer:error element");
    }

    /// <summary>Source completion completes every open group and then the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_CompletesEveryOpenGroupThenTheOuterSequence()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, static _ => Signal.Silent<int>());
        source.OnNext("a1");
        source.OnNext("b1");

        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 open gb gb:b1 ga:done gb:done outer:done");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Source failure faults every open group and then the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_FaultsEveryOpenGroupThenTheOuterSequence()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, static _ => Signal.Silent<int>());
        source.OnNext("a1");

        source.OnError(new InvalidOperationException("boom"));

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 ga:error boom outer:error boom");
    }

    /// <summary>Ending a group releases its duration subscription, so a late duration signal does nothing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DurationEmits_ReleasesTheDurationSubscription()
    {
        Signal<string> source = new();
        Signal<int> duration = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer, _ => duration);
        source.OnNext("a1");

        duration.OnNext(0);

        await Assert.That(duration.HasObservers).IsFalse();
    }

    /// <summary>Disposing the outer subscription keeps the source alive while a group is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileGroupSubscribed_KeepsSourceUntilGroupIsDisposed()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        var outer = Group(source, observer, static _ => Signal.Silent<int>());
        source.OnNext("a1");

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers;
        source.OnNext("a2");
        observer.Subscriptions[0].Dispose();

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 ga:a2");
    }

    /// <summary>Disposing the outer subscription with no subscribed group releases the source and the duration signals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWithoutSubscribedGroup_ReleasesSourceAndDurations()
    {
        Signal<string> source = new();
        Signal<int> duration = new();
        GroupRecordingWitness<char, string> observer = new() { SubscribeOnOpen = false };
        var outer = Group(source, observer, _ => duration);
        source.OnNext("a1");

        outer.Dispose();

        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(duration.HasObservers).IsFalse();
    }

    /// <summary>A downstream observer failure tears the sink down and reaches the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_OuterObserverThrows_DisposesTheSinkAndRethrows()
    {
        Signal<string> source = new();
        ThrowingWitness<GroupedSignal<char, string>> observer = new(throwOnNext: true);
        using var subscription = new GroupByUntilSignal<string, char, string, int>(source, static v => v[0], static v => v, static _ => Signal.Silent<int>(), null, 0)
            .Subscribe(observer);

        await Assert.That(() => source.OnNext("a1")).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Notifications after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        RecordingWitness<GroupedSignal<char, string>> observer = new();
        GroupByUntilWitness<string, char, string, int> witness = new(observer, static v => v[0], static v => v, static _ => Signal.Silent<int>(), null, 0);
        witness.Dispose();

        witness.OnNext("a1");
        witness.OnCompleted();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(0);
    }

    /// <summary>A second terminal notification is ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_Repeated_ForwardsOnlyTheFirst()
    {
        RecordingWitness<GroupedSignal<char, string>> observer = new();
        GroupByUntilWitness<string, char, string, int> witness = new(observer, static v => v[0], static v => v, static _ => Signal.Silent<int>(), null, 0);

        witness.OnCompleted();
        witness.OnError(new InvalidOperationException("late"));

        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
    }

    /// <summary>A null argument is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        RecordingWitness<GroupedSignal<char, string>> observer = new();

        await Assert.That(static () => new GroupByUntilWitness<string, char, string, int>(null!, static v => v[0], static v => v, static _ => Signal.Silent<int>(), null, 0))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByUntilWitness<string, char, string, int>(observer, null!, static v => v, static _ => Signal.Silent<int>(), null, 0))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByUntilWitness<string, char, string, int>(observer, static v => v[0], null!, static _ => Signal.Silent<int>(), null, 0))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByUntilWitness<string, char, string, int>(observer, static v => v[0], static v => v, null!, null, 0))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a duration signal and records it.</summary>
    /// <param name="durations">The recorded duration signals.</param>
    /// <returns>The duration signal.</returns>
    private static Signal<int> NewDuration(List<Signal<int>> durations)
    {
        Signal<int> duration = new();
        durations.Add(duration);
        return duration;
    }

    /// <summary>Groups a string source by its first character until the supplied duration signal fires.</summary>
    /// <param name="source">The source.</param>
    /// <param name="observer">The observer of the groups.</param>
    /// <param name="durationSelector">Supplies the duration signal of a group.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable Group(
        Signal<string> source,
        IObserver<GroupedSignal<char, string>> observer,
        Func<GroupedSignal<char, string>, IObservable<int>> durationSelector) =>
        new GroupByUntilSignal<string, char, string, int>(source, static v => v[0], static v => v, durationSelector, null, 0).Subscribe(observer);
}
