// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests explicit event handler conversion.</summary>
public partial class SignalFactoriesTests
{
    /// <summary>A delegate shaped unlike the event-handler pattern, so it needs an explicit conversion.</summary>
    /// <param name="value">The broadcast value.</param>
    /// <param name="args">Trailing arguments the bridge ignores.</param>
    public delegate void ValueBroadcast(int value, EventArgs args);

    /// <summary>Verifies conversion work stays inside the event even when notifications are scheduled.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventPatternConversionRunsBeforeObserveOn()
    {
        ManualSequencer sequencer = new();
        EventHandler<EventArgs>? handler = null;
        var eventActive = false;
        var handledInsideEvent = false;
        List<EventPattern<EventArgs>> events = [];
        object sender = new();
        using var subscription = Signal.FromEventPattern<EventHandler<EventArgs>, EventArgs>(
                callback => (eventSender, args) =>
                {
                    handledInsideEvent = eventActive;
                    callback(eventSender, args);
                },
                attached => handler = attached,
                detached => handler -= detached)
            .ObserveOn(sequencer)
            .Subscribe(events.Add);

        eventActive = true;
        handler!(sender, EventArgs.Empty);
        eventActive = false;

        await Assert.That(handledInsideEvent).IsTrue();
        await Assert.That(events).IsEmpty();
        sequencer.RunPending();
        await Assert.That(events.Count).IsEqualTo(One);
        await Assert.That(events[0].Sender).IsSameReferenceAs(sender);
        await Assert.That(events[0].EventArgs).IsSameReferenceAs(EventArgs.Empty);
    }

    /// <summary>Verifies custom delegate conversion and disposal use the exact attached handler.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventPatternConversionDetachesTheConvertedHandlerOnce()
    {
        Action<EventArgs>? attached = null;
        Action<EventArgs>? removed = null;
        var removals = 0;
        List<EventPattern<EventArgs>> events = [];
        var subscription = Signal.FromEventPattern<Action<EventArgs>, EventArgs>(
                callback => args => callback(null, args),
                handler => attached = handler,
                handler =>
                {
                    removed = handler;
                    removals++;
                })
            .Subscribe(events.Add);

        attached!(EventArgs.Empty);
        subscription.Dispose();
        subscription.Dispose();

        await Assert.That(events.Count).IsEqualTo(One);
        await Assert.That(events[0].Sender).IsNull();
        await Assert.That(removed).IsSameReferenceAs(attached);
        await Assert.That(removals).IsEqualTo(One);
    }

    /// <summary>Verifies invalid callback arguments are rejected before subscribing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventPatternConversionRejectsNullCallbacks()
    {
        await Assert.That(static () => Signal.FromEventPattern<EventHandler<EventArgs>, EventArgs>(
            null!,
            static _ => { },
            static _ => { })).Throws<ArgumentNullException>();
        await Assert.That(static () => Signal.FromEventPattern<EventHandler<EventArgs>, EventArgs>(
            static handler => handler,
            null!,
            static _ => { })).Throws<ArgumentNullException>();
        await Assert.That(static () => Signal.FromEventPattern<EventHandler<EventArgs>, EventArgs>(
            static handler => handler,
            static _ => { },
            null!)).Throws<ArgumentNullException>();
    }

    /// <summary>
    /// Verifies the handler-pair constructor attaches on the subscribing thread. The factories all supply a
    /// sequencer argument now, so this is the shape only a caller constructing the signal directly reaches.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventPatternSignalWithoutASequencerAttachesOnTheSubscribingThread()
    {
        EventHandler<EventArgs>? handler = null;
        List<EventPattern<EventArgs>> events = [];
        EventBroadcaster sender = new();
        FromEventPatternSignal<EventHandler<EventArgs>, EventArgs> signal = new(
            attached => handler = attached,
            detached => handler -= detached);

        var subscription = signal.Subscribe(events.Add);
        handler!(sender, EventArgs.Empty);
        subscription.Dispose();

        await Assert.That(events.Count).IsEqualTo(One);
        await Assert.That(events[0].Sender).IsSameReferenceAs(sender);
        await Assert.That(handler).IsNull();
    }

    /// <summary>Verifies the typed-sender overload keeps the sender's static type instead of erasing it to object.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventPatternConversionKeepsTheSenderType()
    {
        EventHandler<EventArgs>? handler = null;
        List<EventPattern<EventBroadcaster, EventArgs>> events = [];
        EventBroadcaster sender = new();
        using var subscription = Signal.FromEventPattern<EventHandler<EventArgs>, EventBroadcaster, EventArgs>(
                static callback => callback,
                attached => handler = attached,
                detached => handler -= detached)
            .Subscribe(events.Add);

        handler!(sender, EventArgs.Empty);

        await Assert.That(events.Count).IsEqualTo(One);
        await Assert.That(events[0].Sender).IsSameReferenceAs(sender);
        await Assert.That(events[0].EventArgs).IsSameReferenceAs(EventArgs.Empty);
    }

    /// <summary>Verifies the argument-only bridge emits the value the callback is handed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventEmitsTheArgumentTheCallbackReceives()
    {
        Action<int>? handler = null;
        List<int> values = [];
        var subscription = Signal.FromEvent<int>(
                attached => handler = attached,
                detached => handler -= detached)
            .Subscribe(values.Add);

        handler!(One);
        handler(Two);
        subscription.Dispose();

        await Assert.That(values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(handler).IsNull();
    }

    /// <summary>Verifies the argument-only bridge converts a custom delegate without reflection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventConversionEmitsTheArgumentOfACustomDelegate()
    {
        ValueBroadcast? handler = null;
        List<int> values = [];
        var removals = 0;
        var subscription = Signal.FromEvent<ValueBroadcast, int>(
                static callback => (value, _) => callback(value),
                attached => handler = attached,
                detached =>
                {
                    handler -= detached;
                    removals++;
                })
            .Subscribe(values.Add);

        handler!(One, EventArgs.Empty);
        subscription.Dispose();
        subscription.Dispose();

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(removals).IsEqualTo(One);
    }

    /// <summary>Verifies the sequencer overload attaches and detaches the handler as scheduled work.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventPatternAttachesAndDetachesOnTheSequencer()
    {
        ManualSequencer sequencer = new();
        EventHandler<EventArgs>? handler = null;
        var attachments = 0;
        var detachments = 0;
        List<EventPattern<EventArgs>> events = [];
        EventBroadcaster sender = new();
        var subscription = Signal.FromEventPattern<EventArgs>(
                attached =>
                {
                    handler = attached;
                    attachments++;
                },
                detached =>
                {
                    handler -= detached;
                    detachments++;
                },
                sequencer)
            .Subscribe(events.Add);

        await Assert.That(attachments).IsEqualTo(0);

        sequencer.RunPending();
        await Assert.That(attachments).IsEqualTo(One);

        handler!(sender, EventArgs.Empty);
        await Assert.That(events.Count).IsEqualTo(One);

        subscription.Dispose();
        await Assert.That(detachments).IsEqualTo(0);

        sequencer.RunPending();
        await Assert.That(detachments).IsEqualTo(One);
        await Assert.That(handler).IsNull();
    }

    /// <summary>Verifies a subscription torn down before the sequencer ran the attach never attaches the handler.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventPatternOnASequencerCancelsAPendingAttachOnDisposal()
    {
        ManualSequencer sequencer = new();
        var attachments = 0;
        var subscription = Signal.FromEventPattern<EventArgs>(
                _ => attachments++,
                static _ => { },
                sequencer)
            .Subscribe(static _ => { });

        subscription.Dispose();
        sequencer.RunPending();

        await Assert.That(attachments).IsEqualTo(0);
    }

    /// <summary>Verifies the argument-only and typed-sender overloads reject null callbacks.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEventAndTypedSenderOverloadsRejectNullCallbacks()
    {
        await Assert.That(static () => Signal.FromEvent<Action<int>, int>(
            null!,
            static _ => { },
            static _ => { })).Throws<ArgumentNullException>();
        await Assert.That(static () => Signal.FromEvent<Action<int>, int>(
            static callback => callback,
            null!,
            static _ => { })).Throws<ArgumentNullException>();
        await Assert.That(static () => Signal.FromEvent<Action<int>, int>(
            static callback => callback,
            static _ => { },
            null!)).Throws<ArgumentNullException>();
        await Assert.That(static () => Signal.FromEventPattern<EventHandler<EventArgs>, EventBroadcaster, EventArgs>(
            null!,
            static _ => { },
            static _ => { })).Throws<ArgumentNullException>();
    }

    /// <summary>A sender used to check the typed-sender event pattern keeps its static type.</summary>
    [System.Diagnostics.DebuggerDisplay("EventBroadcaster: Id = {Id}")]
    public sealed class EventBroadcaster
    {
        /// <summary>Gets the identity this broadcaster is recognised by.</summary>
        public Guid Id { get; } = Guid.NewGuid();
    }
}
