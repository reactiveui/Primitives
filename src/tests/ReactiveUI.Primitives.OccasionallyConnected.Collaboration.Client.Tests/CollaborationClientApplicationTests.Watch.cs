// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Builder;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>Real watch command lifecycle tests for <see cref="CollaborationClientApplication"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>Verifies a real remote activity wakes the watch output and cancellation releases SQLite.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WatchPrintsRemoteActivityThenCancelsAndReleasesStore()
    {
        using var lease = new CollaborationClientDatabaseLease();
        var app = CollaborationServerExample.CreateWebApplication(CreateServerOptions(lease.ServerPath));
        try
        {
            await StartServerAsync(app).ConfigureAwait(false);
            var boundUri = new Uri(GetBoundAddress(app.Services));
            await AssertWatchActivityAndReleaseAsync(boundUri, lease).ConfigureAwait(false);
        }
        finally
        {
            await StopAndDisposeServerAsync(app).ConfigureAwait(false);
        }
    }

    /// <summary>Runs the watched client and verifies cancellation releases its owned SQLite handle.</summary>
    /// <param name="boundUri">The live server endpoint.</param>
    /// <param name="lease">The isolated database paths.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertWatchActivityAndReleaseAsync(Uri boundUri, CollaborationClientDatabaseLease lease)
    {
        var command = CreatePublishCommand(boundUri, lease.ClientAPath, TokenA, ClientA, OnlineStatus, OfflineTitle)
            with { Kind = CollaborationClientCommandKind.Watch };
        var output = new ActivitySignalWriter(OnlineStatus);
        var cancellation = new CancellationTokenSource(WaitTimeout);
        var watch = CollaborationClientApplication.RunAsync(command, output, cancellation.Token);
        try
        {
            await using var publisher = await OpenClientAsync(boundUri, lease.ClientBPath, TokenB, ClientB)
                .ConfigureAwait(false);
            await publisher.StartAsync(CancellationToken.None).ConfigureAwait(false);
            _ = await publisher.PublishAsync(
                    new ActivityUpdate { Status = OnlineStatus, Title = OfflineTitle, TitleSpecified = true },
                    CancellationToken.None)
                .ConfigureAwait(false);
            var line = await output.WaitForActivityAsync(WaitTimeout).ConfigureAwait(false);
            await cancellation.CancelAsync().ConfigureAwait(false);
            TaskCanceledException? canceled = null;
            try
            {
                _ = await watch.ConfigureAwait(false);
            }
            catch (TaskCanceledException exception)
            {
                canceled = exception;
            }

            await Assert.That(line.Contains($"{OnlineStatus} | {OfflineTitle}", StringComparison.Ordinal)).IsTrue();
            await Assert.That(canceled?.CancellationToken).IsEqualTo(cancellation.Token);
            await using var exclusive = File.Open(lease.ClientAPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            await Assert.That(exclusive.Length).IsGreaterThan(0);
            await publisher.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            try
            {
                _ = await watch.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                await output.DisposeAsync().ConfigureAwait(false);
                cancellation.Dispose();
            }
        }
    }

    /// <summary>Signals when the public watch output prints the requested activity.</summary>
    private sealed class ActivitySignalWriter : StringWriter
    {
        /// <summary>The status prefix that indicates a remote activity was observed.</summary>
        private readonly string _statusPrefix;

        /// <summary>Completes when the requested activity line is printed.</summary>
        private readonly TaskCompletionSource<string> _activity = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Initializes a new instance of the <see cref="ActivitySignalWriter"/> class.</summary>
        /// <param name="status">The expected status.</param>
        internal ActivitySignalWriter(string status)
            : base(CultureInfo.InvariantCulture)
        {
            _statusPrefix = $"{status} | ";
        }

        /// <inheritdoc />
        public override void WriteLine(string? value)
        {
            base.WriteLine(value);
            if (value is not null && value.StartsWith(_statusPrefix, StringComparison.Ordinal))
            {
                _ = _activity.TrySetResult(value);
            }
        }

        /// <summary>Waits for the requested activity line.</summary>
        /// <param name="timeout">The finite test timeout.</param>
        /// <returns>The printed line.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task<string> WaitForActivityAsync(TimeSpan timeout) => _activity.Task.WaitAsync(timeout);
    }
}
