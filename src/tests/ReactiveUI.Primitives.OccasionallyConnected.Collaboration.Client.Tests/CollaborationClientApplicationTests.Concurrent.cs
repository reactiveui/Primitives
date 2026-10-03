// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.AspNetCore.Builder;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>Subscriber isolation and stale conflict tests for <see cref="CollaborationClientApplication"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>The status shared by stale concurrent activity updates.</summary>
    private const string ConcurrentStatus = "concurrent-ready";

    /// <summary>The title supplied by the client that reaches the server first.</summary>
    private const string ConcurrentTitle = "Concurrent title from client A";

    /// <summary>The details supplied by the stale client.</summary>
    private const string ConcurrentDetails = "Concurrent details from stale client B";

    /// <summary>Verifies one blocked subscriber cannot prevent another subscriber from observing sync.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SlowLocalSubscriberDoesNotBlockFastSubscriberAndCleanupReleasesGate()
    {
        using var lease = new CollaborationClientDatabaseLease();
        WebApplication? app = null;
        try
        {
            app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            await VerifySlowSubscriberIsolationAsync(lease, boundUri).ConfigureAwait(false);
        }
        finally
        {
            if (app is not null)
            {
                await StopAndDisposeServerAsync(app).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a stale publish merges with newer canonical state without conflict telemetry.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StaleClientPublishMergesWithAcceptedCanonicalStateWithoutConflict()
    {
        using var lease = new CollaborationClientDatabaseLease();
        WebApplication? app = null;
        try
        {
            app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            await VerifyStaleClientMergeAsync(lease, boundUri).ConfigureAwait(false);
        }
        finally
        {
            if (app is not null)
            {
                await StopAndDisposeServerAsync(app).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies slow subscriber isolation against a real client and server.</summary>
    /// <param name="lease">The SQLite database lease.</param>
    /// <param name="boundUri">The bound server URI.</param>
    /// <returns>The assertion task.</returns>
    private static async Task VerifySlowSubscriberIsolationAsync(
        CollaborationClientDatabaseLease lease,
        Uri boundUri)
    {
        await using var client = await OpenClientAsync(
            boundUri,
            lease.ClientBPath,
            TokenB,
            ClientB).ConfigureAwait(false);
        BlockingActivityObserver? slow = null;
        IDisposable? slowSubscription = null;
        try
        {
            using var telemetry = new ActivityTelemetry(client.Activity);
            var fast = new RecordingObserver<ActivityView>(TelemetryCapacity);
            using var fastSubscription = client.Activity.Local.Subscribe(fast);
            slow = new(IsSlowObserverBlockedView);
            slowSubscription = client.Activity.Local.Subscribe(slow);
            await StartSingleClientAsync(client).ConfigureAwait(false);
            await VerifySlowSubscriberPublishProgressAsync(client, telemetry, fast, slow).ConfigureAwait(false);
        }
        finally
        {
            slow?.Release();
            slowSubscription?.Dispose();
            slow?.Dispose();
        }
    }

    /// <summary>Verifies fast observer progress while the slow observer gate is held.</summary>
    /// <param name="client">The client session.</param>
    /// <param name="telemetry">The activity telemetry.</param>
    /// <param name="fast">The fast observer.</param>
    /// <param name="slow">The blocking observer.</param>
    /// <returns>The assertion task.</returns>
    private static async Task VerifySlowSubscriberPublishProgressAsync(
        CollaborationClientSession client,
        ActivityTelemetry telemetry,
        RecordingObserver<ActivityView> fast,
        BlockingActivityObserver slow)
    {
        var blockedReceipt = await PublishClientBActivityAsync(client, "slow-blocked", "Slow blocked", "first")
            .ConfigureAwait(false);
        var blocked = await slow.WaitUntilBlockedAsync().ConfigureAwait(false);
        await Assert.That(blocked.AcceptedOperationId).IsEqualTo(ToOperationText(blockedReceipt));
        var finalReceipt = await PublishClientBActivityAsync(client, "slow-final", "Slow final", "second")
            .ConfigureAwait(false);
        var fastFinal = await fast.WaitForAsync(value => IsClientBFinalView(value, finalReceipt), WaitTimeout)
            .ConfigureAwait(false);
        _ = await telemetry.Operations.WaitForAsync(IsSynchronized(finalReceipt), WaitTimeout)
            .ConfigureAwait(false);
        slow.Release();
        await slow.WaitUntilReleasedAsync().ConfigureAwait(false);
        var slowFinal = await slow.WaitForAsync(value => IsClientBFinalView(value, finalReceipt)).ConfigureAwait(false);
        await StopSingleClientAsync(client).ConfigureAwait(false);
        await AssertNoTerminalStreamFailuresAsync(telemetry).ConfigureAwait(false);
        await Assert.That(fastFinal.AcceptedClientId).IsEqualTo(ClientB);
        await Assert.That(slow.BlockedCount).IsEqualTo(1);
        await Assert.That(slowFinal).IsEqualTo(fastFinal);
    }

    /// <summary>Verifies a stale client publish merges with a newer accepted canonical state.</summary>
    /// <param name="lease">The SQLite database lease.</param>
    /// <param name="boundUri">The bound server URI.</param>
    /// <returns>The assertion task.</returns>
    private static async Task VerifyStaleClientMergeAsync(
        CollaborationClientDatabaseLease lease,
        Uri boundUri)
    {
        await using var clientA = await OpenClientAsync(
            boundUri,
            lease.ClientAPath,
            TokenA,
            ClientA).ConfigureAwait(false);
        using var telemetryA = new ActivityTelemetry(clientA.Activity);
        _ = await SeedInitialStaleClientAsync(lease, boundUri, clientA, telemetryA).ConfigureAwait(false);
        var titleReceipt = await PublishClientATitleAsync(clientA).ConfigureAwait(false);
        _ = await telemetryA.Operations.WaitForAsync(IsSynchronized(titleReceipt), WaitTimeout).ConfigureAwait(false);
        _ = await telemetryA.Local.WaitForAsync(value => IsClientATitleView(value, titleReceipt), WaitTimeout)
            .ConfigureAwait(false);
        await VerifyStaleClientDetailsMergeAsync(lease, boundUri, clientA, telemetryA).ConfigureAwait(false);
    }

    /// <summary>Seeds the stale client's durable store and closes its first session.</summary>
    /// <param name="lease">The SQLite database lease.</param>
    /// <param name="boundUri">The bound server URI.</param>
    /// <param name="clientA">The first client session.</param>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <returns>The seed receipt.</returns>
    private static async Task<PublishReceipt> SeedInitialStaleClientAsync(
        CollaborationClientDatabaseLease lease,
        Uri boundUri,
        CollaborationClientSession clientA,
        ActivityTelemetry telemetryA)
    {
        await using var initialClientB = await OpenClientAsync(
            boundUri,
            lease.ClientBPath,
            TokenB,
            ClientB).ConfigureAwait(false);
        using var initialTelemetryB = new ActivityTelemetry(initialClientB.Activity);
        return await SeedInitialAndStopSecondClientAsync(
                clientA,
                initialClientB,
                telemetryA,
                initialTelemetryB)
            .ConfigureAwait(false);
    }

    /// <summary>Publishes the stale update and verifies merged convergence.</summary>
    /// <param name="lease">The SQLite database lease.</param>
    /// <param name="boundUri">The bound server URI.</param>
    /// <param name="clientA">The first client session.</param>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <returns>The assertion task.</returns>
    private static async Task VerifyStaleClientDetailsMergeAsync(
        CollaborationClientDatabaseLease lease,
        Uri boundUri,
        CollaborationClientSession clientA,
        ActivityTelemetry telemetryA)
    {
        await using var staleClientB = await OpenClientAsync(
            boundUri,
            lease.ClientBPath,
            TokenB,
            ClientB).ConfigureAwait(false);
        using var staleTelemetryB = new ActivityTelemetry(staleClientB.Activity);
        var staleReceipt = await PublishClientBDetailsAsync(staleClientB).ConfigureAwait(false);
        _ = await staleTelemetryB.Local.WaitForAsync(
                value => IsStalePendingDetailsView(value, staleReceipt),
                WaitTimeout)
            .ConfigureAwait(false);
        await StartSingleClientAsync(staleClientB).ConfigureAwait(false);
        await WaitForMergedStaleClientConvergenceAsync(telemetryA, staleTelemetryB, staleReceipt)
            .ConfigureAwait(false);
        await StopClientsAsync(clientA, staleClientB).ConfigureAwait(false);
        await AssertNoTerminalStreamFailuresAsync(telemetryA, staleTelemetryB).ConfigureAwait(false);
        await Assert.That(staleTelemetryB.Operations.Count(IsConflict(staleReceipt))).IsEqualTo(0);
    }

    /// <summary>Waits for both clients to observe the stale client's merged canonical state.</summary>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <param name="telemetryB">The stale client telemetry.</param>
    /// <param name="receipt">The stale client receipt.</param>
    /// <returns>The assertion task.</returns>
    private static async Task WaitForMergedStaleClientConvergenceAsync(
        ActivityTelemetry telemetryA,
        ActivityTelemetry telemetryB,
        PublishReceipt receipt)
    {
        var finalA = await telemetryA.Local.WaitForAsync(
                value => IsMergedConcurrentView(value, receipt),
                WaitTimeout)
            .ConfigureAwait(false);
        var finalB = await telemetryB.Local.WaitForAsync(
                value => IsMergedConcurrentView(value, receipt),
                WaitTimeout)
            .ConfigureAwait(false);
        _ = await telemetryA.Remote.WaitForAsync(message => IsRemoteMergedConcurrentDetailsUpdate(message, receipt), WaitTimeout)
            .ConfigureAwait(false);
        _ = await telemetryB.Operations.WaitForAsync(IsSynchronized(receipt), WaitTimeout)
            .ConfigureAwait(false);
        await AssertMergedConcurrentViewsAsync(finalA, finalB, receipt).ConfigureAwait(false);
    }

    /// <summary>Seeds the initial canonical state and closes the second client.</summary>
    /// <param name="clientA">The first client session.</param>
    /// <param name="clientB">The second client session.</param>
    /// <param name="telemetryA">The first client telemetry.</param>
    /// <param name="telemetryB">The second client telemetry.</param>
    /// <returns>The seed receipt.</returns>
    private static async Task<PublishReceipt> SeedInitialAndStopSecondClientAsync(
        CollaborationClientSession clientA,
        CollaborationClientSession clientB,
        ActivityTelemetry telemetryA,
        ActivityTelemetry telemetryB)
    {
        await StartClientsAsync(clientA, clientB).ConfigureAwait(false);
        var seed = await PublishInitialConnectedActivityAndAssertConvergenceAsync(
                clientB,
                telemetryA.Local,
                telemetryB.Local)
            .ConfigureAwait(false);
        await StopSingleClientAsync(clientB).ConfigureAwait(false);
        await AssertNoTerminalStreamFailuresAsync(telemetryB).ConfigureAwait(false);
        return seed;
    }

    /// <summary>Publishes one activity update from client B.</summary>
    /// <param name="client">The client session.</param>
    /// <param name="status">The activity status.</param>
    /// <param name="title">The activity title.</param>
    /// <param name="details">The activity details.</param>
    /// <returns>The publish receipt.</returns>
    private static async Task<PublishReceipt> PublishClientBActivityAsync(
        CollaborationClientSession client,
        string status,
        string title,
        string details)
    {
        using var cancellation = CreateWaitCancellation();
        return await client.PublishAsync(
                new() { Status = status, Title = title, TitleSpecified = true, Details = details, DetailsSpecified = true },
                cancellation.Token)
            .ConfigureAwait(false);
    }

    /// <summary>Publishes the first stale conflict title update from client A.</summary>
    /// <param name="client">The client session.</param>
    /// <returns>The publish receipt.</returns>
    private static async Task<PublishReceipt> PublishClientATitleAsync(CollaborationClientSession client)
    {
        using var cancellation = CreateWaitCancellation();
        return await client.PublishAsync(
                new() { Status = ConcurrentStatus, Title = ConcurrentTitle, TitleSpecified = true },
                cancellation.Token)
            .ConfigureAwait(false);
    }

    /// <summary>Publishes the stale details update from client B.</summary>
    /// <param name="client">The client session.</param>
    /// <returns>The publish receipt.</returns>
    private static async Task<PublishReceipt> PublishClientBDetailsAsync(CollaborationClientSession client)
    {
        using var cancellation = CreateWaitCancellation();
        return await client.PublishAsync(
                new() { Status = ConcurrentStatus, Details = ConcurrentDetails, DetailsSpecified = true },
                cancellation.Token)
            .ConfigureAwait(false);
    }

    /// <summary>Verifies both views converged on the stale client's accepted canonical state.</summary>
    /// <param name="clientA">The first client view.</param>
    /// <param name="clientB">The stale client view.</param>
    /// <param name="receipt">The stale client receipt.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMergedConcurrentViewsAsync(
        ActivityView clientA,
        ActivityView clientB,
        PublishReceipt receipt)
    {
        await Assert.That(clientA.IsPending).IsFalse();
        await Assert.That(clientB.IsPending).IsFalse();
        await Assert.That(clientA.Status).IsEqualTo(ConcurrentStatus);
        await Assert.That(clientB.Status).IsEqualTo(ConcurrentStatus);
        await Assert.That(clientA.Title).IsEqualTo(ConcurrentTitle);
        await Assert.That(clientB.Title).IsEqualTo(ConcurrentTitle);
        await Assert.That(clientA.Details).IsEqualTo(ConcurrentDetails);
        await Assert.That(clientB.Details).IsEqualTo(ConcurrentDetails);
        await Assert.That(clientA.AcceptedOperationId).IsEqualTo(ToOperationText(receipt));
        await Assert.That(clientB.AcceptedOperationId).IsEqualTo(ToOperationText(receipt));
        await Assert.That(clientB.AcceptedVersion).IsEqualTo(clientA.AcceptedVersion);
        await Assert.That(clientB.AcceptedClientId).IsEqualTo(ClientB);
    }

    /// <summary>Determines whether the slow observer should block on this view.</summary>
    /// <param name="value">The view.</param>
    /// <returns>Whether the view is the blocked view.</returns>
    private static bool IsSlowObserverBlockedView(ActivityView value) =>
        string.Equals(value.Status, "slow-blocked", StringComparison.Ordinal)
        && string.Equals(value.Title, "Slow blocked", StringComparison.Ordinal);

    /// <summary>Determines whether the view is the final client B slow-observer update.</summary>
    /// <param name="value">The view.</param>
    /// <param name="receipt">The final receipt.</param>
    /// <returns>Whether the view matches the final update.</returns>
    private static bool IsClientBFinalView(ActivityView value, PublishReceipt receipt) =>
        !value.IsPending
        && string.Equals(value.AcceptedOperationId, ToOperationText(receipt), StringComparison.Ordinal)
        && string.Equals(value.Status, "slow-final", StringComparison.Ordinal)
        && string.Equals(value.Title, "Slow final", StringComparison.Ordinal);

    /// <summary>Determines whether the view is the accepted client A title update.</summary>
    /// <param name="value">The view.</param>
    /// <param name="receipt">The title receipt.</param>
    /// <returns>Whether the view matches the title update.</returns>
    private static bool IsClientATitleView(ActivityView value, PublishReceipt receipt) =>
        !value.IsPending
        && string.Equals(value.AcceptedOperationId, ToOperationText(receipt), StringComparison.Ordinal)
        && string.Equals(value.Status, ConcurrentStatus, StringComparison.Ordinal)
        && string.Equals(value.Title, ConcurrentTitle, StringComparison.Ordinal)
        && string.Equals(value.Details, OnlineDetails, StringComparison.Ordinal);

    /// <summary>Determines whether the stale client still has its original title before reconnecting.</summary>
    /// <param name="value">The view.</param>
    /// <param name="receipt">The stale receipt.</param>
    /// <returns>Whether the view proves the publish was made from stale local state.</returns>
    private static bool IsStalePendingDetailsView(ActivityView value, PublishReceipt receipt) =>
        value.IsPending
        && string.Equals(value.AcceptedOperationId, ToOperationText(receipt), StringComparison.Ordinal)
        && string.Equals(value.Status, ConcurrentStatus, StringComparison.Ordinal)
        && string.Equals(value.Title, OfflineTitle, StringComparison.Ordinal)
        && string.Equals(value.Details, ConcurrentDetails, StringComparison.Ordinal);

    /// <summary>Determines whether the view contains the merged concurrent canonical state.</summary>
    /// <param name="value">The view.</param>
    /// <param name="receipt">The stale receipt.</param>
    /// <returns>Whether the view matches the merged state.</returns>
    private static bool IsMergedConcurrentView(ActivityView value, PublishReceipt receipt) =>
        !value.IsPending
        && string.Equals(value.AcceptedOperationId, ToOperationText(receipt), StringComparison.Ordinal)
        && string.Equals(value.Status, ConcurrentStatus, StringComparison.Ordinal)
        && string.Equals(value.Title, ConcurrentTitle, StringComparison.Ordinal)
        && string.Equals(value.Details, ConcurrentDetails, StringComparison.Ordinal);

    /// <summary>Determines whether a remote message carries the merged canonical details update.</summary>
    /// <param name="message">The remote message.</param>
    /// <param name="receipt">The stale client receipt.</param>
    /// <returns>Whether the message matches the details update.</returns>
    private static bool IsRemoteMergedConcurrentDetailsUpdate(RemoteMessage<ActivityUpdate> message, PublishReceipt receipt) =>
        string.Equals(message.Value.Status, ConcurrentStatus, StringComparison.Ordinal)
        && message.Value.TitleSpecified
        && string.Equals(message.Value.Title, ConcurrentTitle, StringComparison.Ordinal)
        && message.Value.DetailsSpecified
        && string.Equals(message.Value.Details, ConcurrentDetails, StringComparison.Ordinal)
        && string.Equals(message.Value.AcceptedOperationId, ToOperationText(receipt), StringComparison.Ordinal);

    /// <summary>Blocks one activity observer callback until the test releases it.</summary>
    /// <param name="shouldBlock">The predicate that selects the callback to block.</param>
    private sealed class BlockingActivityObserver(Func<ActivityView, bool> shouldBlock) : IObserver<ActivityView>, IDisposable
    {
        /// <summary>Protects terminal observer state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Completes when the selected callback enters the blocked section.</summary>
        private readonly TaskCompletionSource<ActivityView> _blocked =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completes when the selected callback leaves the blocked section.</summary>
        private readonly TaskCompletionSource<ActivityView> _released =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completes when cleanup releases the blocked callback.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Records views so the test can verify eventual delivery to the slow observer.</summary>
        private readonly RecordingObserver<ActivityView> _views = new(TelemetryCapacity);

        /// <summary>Stores the terminal observer error.</summary>
        private Exception? _terminalError;

        /// <summary>Stores the blocked callback count.</summary>
        private int _blockedCount;

        /// <summary>Gets the blocked callback count.</summary>
        internal int BlockedCount => Volatile.Read(ref _blockedCount);

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                _terminalError ??= error;
                _ = _blocked.TrySetCanceled();
                _ = _released.TrySetCanceled();
            }
        }

        /// <inheritdoc />
        public void OnNext(ActivityView value)
        {
            _views.OnNext(value);
            if (!shouldBlock(value) || Interlocked.CompareExchange(ref _blockedCount, 1, 0) != 0)
            {
                return;
            }

            _ = _blocked.TrySetResult(value);
            _release.Task.GetAwaiter().GetResult();
            _ = _released.TrySetResult(value);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Release();

        /// <summary>Waits for a view after the blocked callback is released.</summary>
        /// <param name="predicate">The expected view predicate.</param>
        /// <returns>The matching view.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task<ActivityView> WaitForAsync(Func<ActivityView, bool> predicate) => _views.WaitForAsync(predicate, WaitTimeout);

        /// <summary>Releases the blocked callback.</summary>
        internal void Release() => _ = _release.TrySetResult();

        /// <summary>Waits until the observer callback is blocked.</summary>
        /// <returns>The blocked view.</returns>
        internal Task<ActivityView> WaitUntilBlockedAsync()
        {
            ThrowIfTerminalError();
            return _blocked.Task.WaitAsync(WaitTimeout);
        }

        /// <summary>Waits until the blocked callback is released.</summary>
        /// <returns>The released view.</returns>
        internal Task<ActivityView> WaitUntilReleasedAsync()
        {
            ThrowIfTerminalError();
            return _released.Task.WaitAsync(WaitTimeout);
        }

        /// <summary>Throws the retained terminal observer error.</summary>
        /// <exception cref="InvalidOperationException">The observer recorded a terminal error.</exception>
        private void ThrowIfTerminalError()
        {
            lock (_gate)
            {
                if (_terminalError is not null)
                {
                    throw new InvalidOperationException(
                        "The blocking observer ended with a terminal error.",
                        _terminalError);
                }
            }
        }
    }
}
