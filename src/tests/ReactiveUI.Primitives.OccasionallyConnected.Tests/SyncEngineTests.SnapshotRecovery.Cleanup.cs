// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery cleanup helpers for <see cref="SyncEngineTests"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Releases gates, joins queued stop, and disposes created streams.</summary>
    /// <param name="releaseGap">The gate that allows the subscription gap to continue.</param>
    /// <param name="releaseRecovery">The gate that allows the remote recovery response to complete.</param>
    /// <param name="stopQueued">The queued stream stop task, if one was started.</param>
    /// <param name="queued">The queued stream to dispose after gates are released.</param>
    /// <param name="next">The next stream to dispose after gates are released.</param>
    /// <param name="releaseCommit">The commit gate to release, if the fixture installed one.</param>
    /// <returns>The cleanup task.</returns>
    private static async Task ReleaseSnapshotRecoveryFixtureAsync(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Task? stopQueued,
        IAsyncDisposable? queued,
        IAsyncDisposable? next,
        TaskCompletionSource? releaseCommit)
    {
        _ = releaseGap.TrySetResult();
        _ = releaseRecovery.TrySetResult();
        _ = releaseCommit?.TrySetResult();
        try
        {
            if (stopQueued is not null)
            {
                await ObserveTaskCompletionAsync(stopQueued).ConfigureAwait(false);
            }
        }
        finally
        {
            await DisposeCreatedSnapshotRecoveryStreamsAsync(next, queued).ConfigureAwait(false);
        }
    }

    /// <summary>Releases recovery gates and observes a fixture task after release.</summary>
    /// <param name="releaseGap">The gate that allows the subscription gap to continue.</param>
    /// <param name="releaseRecovery">The gate that allows the remote recovery response to complete.</param>
    /// <param name="task">The task to observe after gates are released.</param>
    /// <returns>The cleanup task.</returns>
    private static async Task ReleaseSnapshotRecoveryFixtureAsync(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery,
        Task? task)
    {
        _ = releaseGap.TrySetResult();
        _ = releaseRecovery.TrySetResult();
        if (task is not null)
        {
            await ObserveTaskCompletionAsync(task).ConfigureAwait(false);
        }
    }

    /// <summary>Disposes created snapshot recovery streams while attempting every cleanup step.</summary>
    /// <param name="next">The next stream to dispose first.</param>
    /// <param name="queued">The queued stream to dispose after the next stream.</param>
    /// <returns>The disposal task.</returns>
    private static async Task DisposeCreatedSnapshotRecoveryStreamsAsync(
        IAsyncDisposable? next,
        IAsyncDisposable? queued)
    {
        try
        {
            if (next is not null)
            {
                await next.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            if (queued is not null)
            {
                await queued.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
