// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the router that delivers a sliced sequence in posting order outside the caller's lock.</summary>
public class SliceRouterTests
{
    /// <summary>Nothing is delivered until the router is flushed, and then everything arrives in posting order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Flush_DeliversPostedNotificationsInOrder()
    {
        SharedSubscription owner = new();
        WindowRecordingWitness<string> outer = new();
        SliceRouter<IObservable<string>, string> router = new(outer, owner);
        SliceWindow<string> window = new(owner);

        router.Open(window);
        router.Publish(window, "a");
        router.Publish(window, "b");
        router.Complete(window);
        var beforeFlush = outer.Text;
        router.Flush();

        await Assert.That(beforeFlush).IsEqualTo(string.Empty);
        await Assert.That(outer.Text).IsEqualTo("open w0 w0:a w0:b w0:done");
    }

    /// <summary>A window fault is delivered to the window.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Flush_FaultedWindow_DeliversTheError()
    {
        SharedSubscription owner = new();
        WindowRecordingWitness<string> outer = new();
        SliceRouter<IObservable<string>, string> router = new(outer, owner);
        SliceWindow<string> window = new(owner);

        router.Open(window);
        router.Fault(window, new InvalidOperationException("boom"));
        router.Flush();

        await Assert.That(outer.Text).IsEqualTo("open w0 w0:error boom");
    }

    /// <summary>Finishing terminates each window and then the outer observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Finish_TerminatesWindowsThenTheOuterObserver()
    {
        SharedSubscription owner = new();
        WindowRecordingWitness<string> outer = new();
        SliceRouter<IObservable<string>, string> router = new(outer, owner);
        SliceWindow<string> first = new(owner);
        SliceWindow<string> second = new(owner);
        router.Open(first);
        router.Open(second);
        router.Flush();

        router.Finish([first, second], null);
        router.Flush();

        await Assert.That(outer.Text).IsEqualTo("open w0 open w1 w0:done w1:done outer:done");
    }

    /// <summary>Finishing with an error faults each window and then the outer observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Finish_WithError_FaultsWindowsThenTheOuterObserver()
    {
        SharedSubscription owner = new();
        WindowRecordingWitness<string> outer = new();
        SliceRouter<IObservable<string>, string> router = new(outer, owner);
        SliceWindow<string> window = new(owner);
        router.Open(window);
        router.Flush();

        router.Finish([window], new InvalidOperationException("boom"));
        router.Flush();

        await Assert.That(outer.Text).IsEqualTo("open w0 w0:error boom outer:error boom");
    }

    /// <summary>Nothing posted after the terminal notification is delivered.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Publish_AfterFinish_IsNotDelivered()
    {
        SharedSubscription owner = new();
        WindowRecordingWitness<string> outer = new();
        SliceRouter<IObservable<string>, string> router = new(outer, owner);
        SliceWindow<string> window = new(owner);
        router.Open(window);
        router.Finish([], null);

        router.Publish(window, "a");
        router.Flush();

        await Assert.That(outer.Text).IsEqualTo("open w0 outer:done");
    }

    /// <summary>Completing the outer sequence early leaves later window notifications deliverable.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompleteOuter_LeavesWindowNotificationsDeliverable()
    {
        SharedSubscription owner = new();
        WindowRecordingWitness<string> outer = new();
        SliceRouter<IObservable<string>, string> router = new(outer, owner);
        SliceWindow<string> window = new(owner);
        router.Open(window);
        router.CompleteOuter();
        router.Publish(window, "a");
        router.FinishWindows([window], null);

        router.Flush();

        await Assert.That(outer.Text).IsEqualTo("open w0 outer:done w0:a w0:done");
    }

    /// <summary>Once the owner is disposed, outer notifications stop and window notifications continue.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Flush_AfterOwnerDisposed_StopsOuterNotificationsOnly()
    {
        SharedSubscription owner = new();
        WindowRecordingWitness<string> outer = new();
        SliceRouter<IObservable<string>, string> router = new(outer, owner);
        SliceWindow<string> window = new(owner);
        router.Open(window);
        router.Flush();
        owner.Dispose();

        router.Open(new SliceWindow<string>(owner));
        router.CompleteOuter();
        router.Publish(window, "a");
        router.Finish([window], new InvalidOperationException("boom"));
        router.Flush();

        await Assert.That(outer.Text).IsEqualTo("open w0 w0:a w0:error boom");
    }

    /// <summary>A notification posted by the observer that is being delivered arrives after that observer returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Flush_PostedDuringDelivery_IsDeliveredAfterTheObserverReturns()
    {
        SharedSubscription owner = new();
        List<string> log = [];
        SliceRouter<IObservable<string>, string>? router = null;
        SliceWindow<string> window = new(owner);
        var outer = new CallbackWindowObserver(() =>
        {
            log.Add("open");
            router!.Publish(window, "a");
            router.Flush();
            log.Add("returned");
        });
        router = new(outer, owner);
        using var subscription = window.Subscribe(new CallbackValueObserver(value => log.Add($"value {value}")));

        router.Open(window);
        router.Flush();

        await Assert.That(string.Join(' ', log)).IsEqualTo("open returned value a");
    }

    /// <summary>A null observer or owner is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullArguments_ThrowArgumentNull()
    {
        RecordingWitness<IObservable<string>> outer = new();

        await Assert.That(static () => new SliceRouter<IObservable<string>, string>(null!, new())).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new SliceRouter<IObservable<string>, string>(outer, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Runs a callback for each window.</summary>
    /// <param name="onWindow">The callback.</param>
    private sealed class CallbackWindowObserver(Action onWindow) : IObserver<IObservable<string>>
    {
        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(IObservable<string> value) => onWindow();
    }

    /// <summary>Runs a callback for each value.</summary>
    /// <param name="onValue">The callback.</param>
    private sealed class CallbackValueObserver(Action<string> onValue) : IObserver<string>
    {
        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(string value) => onValue(value);
    }
}
