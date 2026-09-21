// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests <see cref="SynchronizationContextSequencer"/> driven by a <see cref="TimeProvider"/>.</summary>
public class SynchronizationContextSequencerTests
{
    /// <summary>The delay used for scheduled work.</summary>
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    /// <summary>The smallest step of the virtual clock.</summary>
    private static readonly TimeSpan OneTick = TimeSpan.FromTicks(1);

    /// <summary>A sequencer rejects a missing context or provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConstructorRejectsANullContextOrProvider()
    {
        await Assert.That(static () => new SynchronizationContextSequencer(null!, (TimeProvider)new VirtualClock()))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => new SynchronizationContextSequencer(new(), (TimeProvider)null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>The current time and timestamp follow the provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task NowAndTimestampFollowTheProvider()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        TimeProvider provider = clock;
        SynchronizationContextSequencer sequencer = new(new(), provider);
        var start = sequencer.Timestamp;

        clock.AdvanceBy(OneSecond);

        await Assert.That(sequencer.Now).IsEqualTo(DateTimeOffset.UnixEpoch + OneSecond);
        await Assert.That(sequencer.Timestamp - start).IsEqualTo(Sequencer.ToTimestampDelta(OneSecond));
    }

    /// <summary>Delayed work is posted to the context only once the provider is advanced to its timestamp.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DelayedWorkIsPostedWhenTheProviderReachesItsTimestamp()
    {
        VirtualClock clock = new();
        TimeProvider provider = clock;
        InlineContext context = new();
        SynchronizationContextSequencer sequencer = new(context, provider);
        RecordingWorkItem item = new();

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        clock.AdvanceBy(OneSecond - OneTick);

        await Assert.That(context.PostCount).IsEqualTo(0);
        await Assert.That(item.ExecuteCount).IsEqualTo(0);

        clock.AdvanceBy(OneTick);

        await Assert.That(context.PostCount).IsEqualTo(1);
        await Assert.That(item.ExecuteCount).IsEqualTo(1);
    }

    /// <summary>Delayed work cancelled before it is due never reaches the context.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CancelledDelayedWorkIsNotPosted()
    {
        VirtualClock clock = new();
        TimeProvider provider = clock;
        InlineContext context = new();
        SynchronizationContextSequencer sequencer = new(context, provider);
        RecordingWorkItem item = new();

        sequencer.Schedule(item, sequencer.Timestamp + Sequencer.ToTimestampDelta(OneSecond));
        item.Dispose();
        clock.AdvanceBy(OneSecond);

        await Assert.That(context.PostCount).IsEqualTo(0);
        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>A synchronization context that runs posted work inline and counts the posts.</summary>
    private sealed class InlineContext : SynchronizationContext
    {
        /// <summary>Gets the number of posted callbacks.</summary>
        internal int PostCount { get; private set; }

        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state)
        {
            PostCount++;
            d(state);
        }
    }
}
