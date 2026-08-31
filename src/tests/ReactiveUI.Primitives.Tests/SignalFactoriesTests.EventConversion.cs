// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests explicit event handler conversion.</summary>
public partial class SignalFactoriesTests
{
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
}
