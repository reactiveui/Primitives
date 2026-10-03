// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Exercises the deterministic clock and observed event recorder used by the HTTP lost-ACK workflow.</summary>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>The one-shot timer delay and expected periodic tick count.</summary>
    private const int TimerDueSeconds = 2;

    /// <summary>The clock interval that must produce no callbacks.</summary>
    private const int QuietAdvanceSeconds = 5;

    /// <summary>A due time in the past that fires at the next advance.</summary>
    private const int ImmediateDueSeconds = -2;

    /// <summary>The first recorded event and final timer tick count.</summary>
    private const int FirstObservedValue = 3;

    /// <summary>The second recorded event.</summary>
    private const int SecondObservedValue = 7;

    /// <summary>A value never emitted by the recorder.</summary>
    private const int MissingObservedValue = 9;

    /// <summary>The expected number of recorded events.</summary>
    private const int RecordedValueCount = 2;

    /// <summary>The initial UTC instant.</summary>
    private static readonly DateTimeOffset InitialUtc = DateTimeOffset.UnixEpoch.AddDays(1);

    /// <summary>One-shot timers fire at their due instant, then stay quiet.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OneShotTimerFiresOnceAndStopsAfterDisposal()
    {
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(InitialUtc);
        var fired = 0;
        var timer = clock.CreateTimer(_ => fired++, null, TimeSpan.FromSeconds(TimerDueSeconds), Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.That(fired).IsEqualTo(0);
        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.That(fired).IsEqualTo(1);
        clock.Advance(TimeSpan.FromSeconds(QuietAdvanceSeconds));
        await Assert.That(fired).IsEqualTo(1);
        timer.Dispose();
        await Assert.That(timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan)).IsFalse();
    }

    /// <summary>Periodic and disabled timers follow explicit clock advances.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PeriodicTimerCanBeDisabledAndDisposedAsynchronously()
    {
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(InitialUtc);
        var fired = 0;
        await using var timer = clock.CreateTimer(_ => fired++, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        clock.Advance(TimeSpan.FromSeconds(1));
        clock.Advance(TimeSpan.FromSeconds(1));
        await Assert.That(fired).IsEqualTo(TimerDueSeconds);

        await Assert.That(timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan)).IsTrue();
        clock.Advance(TimeSpan.FromSeconds(QuietAdvanceSeconds));
        await Assert.That(fired).IsEqualTo(TimerDueSeconds);

        await Assert.That(timer.Change(TimeSpan.FromSeconds(ImmediateDueSeconds), Timeout.InfiniteTimeSpan)).IsTrue();
        clock.Advance(TimeSpan.Zero);
        await Assert.That(fired).IsEqualTo(FirstObservedValue);
    }

    /// <summary>The recorder retains events and leaves cancellation visible to its caller.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RecorderRetainsEventsAndCanceledWaitFails()
    {
        var recorder = new DurableHttpLostAckScenario.RecordingObserver<int>();
        recorder.OnCompleted();
        recorder.OnError(new InvalidOperationException("observer diagnostic"));
        recorder.OnNext(FirstObservedValue);
        recorder.OnNext(SecondObservedValue);
        await Assert.That(recorder.Count).IsEqualTo(RecordedValueCount);
        await Assert.That(recorder.CountWhere(static value => value > QuietAdvanceSeconds)).IsEqualTo(1);
        await Assert.That(recorder.Describe(static value => value.ToString(System.Globalization.CultureInfo.InvariantCulture))).IsEqualTo("3,7");
        await Assert.That(await recorder.WaitForAsync(static value => value == SecondObservedValue, CancellationToken.None)).IsEqualTo(SecondObservedValue);

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.That(async () => await recorder.WaitForAsync(static value => value == MissingObservedValue, cancellation.Token)).Throws<OperationCanceledException>();
    }

    /// <summary>A wait begun before publication resumes after the recorder receives the matching event.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RecorderWaitStartedBeforePublicationReceivesEvent()
    {
        var recorder = new DurableHttpLostAckScenario.RecordingObserver<int>();
        var waiting = recorder.WaitForAsync(static value => value == SecondObservedValue, CancellationToken.None);
        await Assert.That(waiting.IsCompleted).IsFalse();
        recorder.OnNext(SecondObservedValue);
        await Assert.That(await waiting).IsEqualTo(SecondObservedValue);
    }

    /// <summary>A reopened writer cannot create a new operation ID.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReopenedWriterOperationIdSourceRejectsNewWork() =>
        await Assert.That(DurableHttpLostAckScenario.ThrowingOperationIdSource.Instance.New).Throws<InvalidOperationException>();
}
