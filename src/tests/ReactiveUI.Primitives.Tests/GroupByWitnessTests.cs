// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the sink that routes values into keyed groups.</summary>
public class GroupByWitnessTests
{
    /// <summary>A new key opens its group before the value that opened it reaches the group.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_NewKey_OpensGroupBeforeDeliveringItsFirstValue()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer);

        source.OnNext("a1");
        source.OnNext("b1");
        source.OnNext("a2");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 open gb gb:b1 ga:a2");
    }

    /// <summary>The element selector projects the value stored in the group.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_ElementSelector_ProjectsGroupValues()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = new GroupBySignal<string, char, string>(source, static v => v[0], static v => v.ToUpperInvariant(), null, 0).Subscribe(observer);

        source.OnNext("a1");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:A1");
    }

    /// <summary>A comparer decides which values share a key.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_Comparer_MergesKeysItTreatsAsEqual()
    {
        const int initialCapacity = 4;
        Signal<string> source = new();
        GroupRecordingWitness<string, string> observer = new();
        using var subscription = new GroupBySignal<string, string, string>(source, static v => v, static v => v, StringComparer.OrdinalIgnoreCase, initialCapacity)
            .Subscribe(observer);

        source.OnNext("a");
        source.OnNext("A");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a ga:A");
    }

    /// <summary>Completion completes every group in the order it opened and then the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_CompletesEveryGroupThenTheOuterSequence()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer);

        source.OnNext("a1");
        source.OnNext("b1");
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 open gb gb:b1 ga:done gb:done outer:done");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>An empty source completes the outer sequence without opening a group.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_WithoutValues_CompletesOnlyTheOuterSequence()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer);

        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("outer:done");
    }

    /// <summary>A source error faults every group and then the outer sequence.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_FaultsEveryGroupThenTheOuterSequence()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = Group(source, observer);

        source.OnNext("a1");
        source.OnNext("b1");
        source.OnError(new InvalidOperationException("boom"));

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 open gb gb:b1 ga:error boom gb:error boom outer:error boom");
    }

    /// <summary>A key selector failure faults every group and the outer sequence and releases the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_KeySelectorThrows_FaultsEverythingAndReleasesTheSource()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = new GroupBySignal<string, char, string>(
            source,
            static v => v == "bad" ? throw new InvalidOperationException("key") : v[0],
            static v => v,
            null,
            0).Subscribe(observer);

        source.OnNext("a1");
        source.OnNext("bad");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 ga:error key outer:error key");
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>An element selector failure delivers the new group and then faults everything.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_ElementSelectorThrows_FaultsTheGroupThatWasJustOpened()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        using var subscription = new GroupBySignal<string, char, string>(source, static v => v[0], static _ => throw new InvalidOperationException("element"), null, 0)
            .Subscribe(observer);

        source.OnNext("a1");

        await Assert.That(observer.Text).IsEqualTo("open ga ga:error element outer:error element");
    }

    /// <summary>A null key faults the sequence instead of throwing at the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_NullKey_FaultsTheOuterSequence()
    {
        Signal<string?> source = new();
        RecordingWitness<GroupedSignal<string, string?>> observer = new();
        using var subscription = new GroupBySignal<string?, string, string?>(source, static v => v!, static v => v, null, 0).Subscribe(observer);

        source.OnNext(null);

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
    }

    /// <summary>Disposing the outer subscription keeps the source alive while a group is subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWhileGroupSubscribed_KeepsSourceUntilEveryGroupIsDisposed()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        var outer = Group(source, observer);
        source.OnNext("a1");
        source.OnNext("b1");

        outer.Dispose();
        var aliveAfterOuter = source.HasObservers;
        source.OnNext("a2");
        observer.Subscriptions[0].Dispose();
        var aliveAfterFirstGroup = source.HasObservers;
        source.OnNext("b2");
        observer.Subscriptions[1].Dispose();

        await Assert.That(aliveAfterOuter).IsTrue();
        await Assert.That(aliveAfterFirstGroup).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 open gb gb:b1 ga:a2 gb:b2");
    }

    /// <summary>Disposing the outer subscription with no subscribed group releases the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_OuterWithoutSubscribedGroup_ReleasesTheSource()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new() { SubscribeOnOpen = false };
        var outer = Group(source, observer);
        source.OnNext("a1");

        outer.Dispose();

        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>A group that opens after the outer subscription is disposed is not delivered to the outer observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterOuterDisposedWithGroupAlive_DoesNotDeliverNewGroups()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new();
        var outer = Group(source, observer);
        source.OnNext("a1");
        outer.Dispose();

        source.OnNext("b1");
        source.OnCompleted();

        await Assert.That(observer.Text).IsEqualTo("open ga ga:a1 ga:done");
    }

    /// <summary>A subscriber that arrives late sees only the values published after it subscribed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_LateToGroup_SeesOnlyLaterValues()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new() { SubscribeOnOpen = false };
        using var outer = Group(source, observer);
        source.OnNext("a1");
        source.OnNext("a2");
        RecordingWitness<string> late = new();

        using var lateSubscription = observer.Groups[0].Subscribe(late);
        source.OnNext("a3");

        await Assert.That(late.Values.SequenceEqual(["a3"])).IsTrue();
    }

    /// <summary>A subscriber that arrives after the group ended is completed at once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_ToEndedGroup_CompletesAtOnce()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new() { SubscribeOnOpen = false };
        using var outer = Group(source, observer);
        source.OnNext("a1");
        source.OnCompleted();
        RecordingWitness<string> late = new();

        using var lateSubscription = observer.Groups[0].Subscribe(late);

        await Assert.That(late.Completed).IsEqualTo(1);
        await Assert.That(late.Values.Count).IsEqualTo(0);
    }

    /// <summary>A subscriber that arrives after the group faulted receives the error at once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_ToFaultedGroup_ReceivesTheErrorAtOnce()
    {
        Signal<string> source = new();
        GroupRecordingWitness<char, string> observer = new() { SubscribeOnOpen = false };
        using var outer = Group(source, observer);
        source.OnNext("a1");
        InvalidOperationException error = new("late");
        source.OnError(error);
        RecordingWitness<string> late = new();

        using var lateSubscription = observer.Groups[0].Subscribe(late);

        await Assert.That(late.Errors.Single()).IsSameReferenceAs(error);
    }

    /// <summary>A value raised while the observer is running is delivered after the observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_RaisedByGroupObserver_IsDeliveredAfterTheObserverReturns()
    {
        Signal<string> source = new();
        List<string> log = [];
        GroupRecordingWitness<char, string> observer = new() { SubscribeOnOpen = false };
        using var outer = Group(source, observer);
        source.OnNext("a1");
        using var reentrant = observer.Groups[0].Subscribe(new CallbackObserver(value =>
        {
            log.Add($"value {value}");
            if (value != "a2")
            {
                return;
            }

            source.OnNext("a3");
            log.Add("returned");
        }));

        source.OnNext("a2");

        await Assert.That(string.Join(' ', log)).IsEqualTo("value a2 returned value a3");
    }

    /// <summary>An observer that throws tears the sink down and the exception reaches the source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_OuterObserverThrows_DisposesTheSinkAndRethrows()
    {
        Signal<string> source = new();
        ThrowingWitness<GroupedSignal<char, string>> observer = new(throwOnNext: true);
        using var subscription = Group(source, observer);

        await Assert.That(() => source.OnNext("a1")).ThrowsExactly<InvalidOperationException>();
        await Assert.That(source.HasObservers).IsFalse();
    }

    /// <summary>Values that arrive after the sink is disposed are ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_AfterDispose_IsIgnored()
    {
        RecordingWitness<GroupedSignal<char, string>> observer = new();
        GroupByWitness<string, char, string> witness = new(observer, static v => v[0], static v => v, null, 0);
        witness.Dispose();

        witness.OnNext("a1");

        await Assert.That(observer.Values.Count).IsEqualTo(0);
    }

    /// <summary>A second terminal notification is ignored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompleted_Repeated_ForwardsOnlyTheFirst()
    {
        RecordingWitness<GroupedSignal<char, string>> observer = new();
        GroupByWitness<string, char, string> witness = new(observer, static v => v[0], static v => v, null, 0);

        witness.OnCompleted();
        witness.OnError(new InvalidOperationException("late"));
        witness.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
    }

    /// <summary>A null observer or selector is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        RecordingWitness<GroupedSignal<char, string>> observer = new();

        await Assert.That(static () => new GroupByWitness<string, char, string>(null!, static v => v[0], static v => v, null, 0)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByWitness<string, char, string>(observer, null!, static v => v, null, 0)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new GroupByWitness<string, char, string>(observer, static v => v[0], null!, null, 0)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Groups a string source by its first character.</summary>
    /// <param name="source">The source.</param>
    /// <param name="observer">The observer of the groups.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IDisposable Group(Signal<string> source, IObserver<GroupedSignal<char, string>> observer) =>
        new GroupBySignal<string, char, string>(source, static v => v[0], static v => v, null, 0).Subscribe(observer);

    /// <summary>Runs a callback for each value.</summary>
    /// <param name="onNext">The callback.</param>
    private sealed class CallbackObserver(Action<string> onNext) : IObserver<string>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(string value) => onNext(value);
    }
}
