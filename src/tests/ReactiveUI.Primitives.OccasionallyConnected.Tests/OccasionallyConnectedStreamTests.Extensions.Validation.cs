// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Convenience helper validation.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>Verifies source and remote adapter helpers require owned input capture.</summary>
    /// <returns>A task that completes when validation assertions finish.</returns>
    [Test]
    public async Task ConvenienceHelpersRejectMissingObserverInputCapture()
    {
        await using var stream = new UnsupportedExternalStream<CounterState, MutableCounterInput>();
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var source = new ManualObservable<MutableCounterInput>();
        var sink = new RecordingObserver<MutableCounterInput>();
        var definition = CreateMutableInputDefinition();

        await Assert.That(() => source.ToOccasionallyConnected(context, definition)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => sink.ToRemoteObserver(context, definition, new RemotePublishOptions { StreamId = Stream }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies adapter construction refuses publish options for a different stream.</summary>
    /// <returns>A task that completes when validation assertions finish.</returns>
    [Test]
    public async Task ToRemoteObserverRejectsMismatchedPublishStreamId()
    {
        await using var stream = new UnsupportedExternalStream<CounterState, MutableCounterInput>();
        await using var context = new ForwardingContext<CounterState, MutableCounterInput>(stream);
        var sink = new RecordingObserver<MutableCounterInput>();
        var definition = CreateMutableInputDefinition() with
        {
            Input = new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = WorkCapacity, BufferCapacityBytes = NotificationCapacityBytes },
            InputCapture = new MutableCounterInputCapture(),
        };

        await Assert.That(() => sink.ToRemoteObserver(context, definition, new RemotePublishOptions { StreamId = new("counter/other") }))
            .ThrowsExactly<InvalidOperationException>();
    }
}
