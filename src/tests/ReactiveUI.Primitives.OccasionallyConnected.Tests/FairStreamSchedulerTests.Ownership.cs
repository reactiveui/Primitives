// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies scheduler ownership and application callback boundaries.</summary>
public sealed partial class FairStreamSchedulerTests
{
    /// <summary>Verifies blocked application clocks do not prevent independent scheduler access.</summary>
    /// <param name="acquire">Whether to block acquisition rather than head admission.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BlockedClockAllowsConcurrentSchedulerAccess(bool acquire)
    {
        using var clock = new BlockingSchedulerClock();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);
        scheduler.Register(new(stream, LightWeight));
        if (acquire)
        {
            scheduler.Ready(stream, NormalPriority, DateTimeOffset.UnixEpoch);
        }

        clock.Block = true;
        var operation = Task.Factory.StartNew(
            static state =>
            {
                var (target, streamId, shouldAcquire) = ((FairStreamScheduler, StreamId, bool))
                    (state ?? throw new InvalidOperationException("The scheduler task state is required."));
                if (shouldAcquire)
                {
                    _ = target.TryAcquire(out _);
                }
                else
                {
                    target.Ready(streamId, NormalPriority, DateTimeOffset.UnixEpoch);
                }
            },
            (scheduler, stream, acquire),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        try
        {
            await clock.Entered.Task.WaitAsync(GuardTimeout);
            var count = await Task.Run(() => scheduler.RegisteredStreamCount).WaitAsync(GuardTimeout);
            await Assert.That(count).IsEqualTo(1);
        }
        finally
        {
            clock.Release();
            await operation.WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Verifies a newly constructed token with the same stream identifier cannot release an acquired head.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ForgedAcquisitionCannotReleaseCurrentHead()
    {
        var clock = new Microsoft.Extensions.Time.Testing.FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);
        scheduler.Register(new(stream, LightWeight));
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());
        var actual = await AcquireAsync(scheduler);
        await Assert.That(() => scheduler.Complete(new(stream))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(scheduler.TryAcquire(out _)).IsFalse();
        scheduler.Complete(actual);
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());
        await Assert.That((await AcquireAsync(scheduler)).StreamId).IsEqualTo(stream);
    }

    /// <summary>An application clock whose callback can be held while another thread accesses the scheduler.</summary>
    private sealed class BlockingSchedulerClock : TimeProvider, IDisposable
    {
        /// <summary>The callback release signal.</summary>
        private readonly ManualResetEventSlim _release = new();

        /// <summary>Gets the callback entry signal.</summary>
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets or sets whether the callback waits.</summary>
        public bool Block { get; set; }

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow()
        {
            if (Block)
            {
                _ = Entered.TrySetResult();
                _release.Wait();
            }

            return DateTimeOffset.UnixEpoch;
        }

        /// <summary>Releases the clock callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release() => _release.Set();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _release.Dispose();
    }
}
