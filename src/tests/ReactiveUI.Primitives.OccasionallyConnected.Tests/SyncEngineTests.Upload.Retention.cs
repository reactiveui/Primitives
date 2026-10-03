// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Tests durable exactly-once expiry after a long offline interval.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>The short retention window from the reviewed server configuration.</summary>
    private const int ReviewedRetentionMinutes = 5;

    /// <summary>The offline interval that exceeds the reviewed retention window.</summary>
    private const int OfflineAfterLostAckDays = 2;

    /// <summary>Verifies restart after a lost ACK cannot resend outside its persisted exactly-once window.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [NotInParallel]
    public async Task ExactlyOnceLostAckAfterDaysOfflineExpiresBeforeAnotherSend()
    {
        var directory = SqliteTestDirectory.Create("oc-exactly-once-long-offline-");
        try
        {
            var path = Path.Combine(directory.FullName, "local.db");
            var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
            var retention = TimeSpan.FromMinutes(ReviewedRetentionMinutes);
            var operation = CreateOperation() with
            {
                Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
            };

            await RunFirstExactlyOnceSqliteUploadAttemptAsync(path, clock, operation, retention);
            clock.Advance(TimeSpan.FromDays(OfflineAfterLostAckDays));
            await AssertExpiredExactlyOnceSqliteUploadDoesNotPrepareAsync(path, clock, operation.OperationId, retention);
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }
}
