// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Checks that the lost-ACK lab preserves primary and cleanup failures.</summary>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>The expected number of independent cleanup failures.</summary>
    private const int CombinedFailureCount = 2;

    /// <summary>External cancellation after real directory allocation still removes the owned directory.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunCancellationAfterDirectoryAllocationCleansOwnedResources()
    {
        var root = Path.Combine(Path.GetTempPath(), $"oc-lost-ack-run-{Guid.NewGuid():N}");
        using var cancellation = new CancellationTokenSource();
        try
        {
            await Assert.That(async () => await DurableHttpLostAckScenario.RunWithRootFactoryAsync(
                () =>
                {
                    _ = Directory.CreateDirectory(root);
                    cancellation.Cancel();
                    return root;
                },
                cancellation.Token)).Throws<OperationCanceledException>();
            await Assert.That(Directory.Exists(root)).IsFalse();
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    /// <summary>A successful workflow surfaces a real filesystem cleanup fault from an open owned directory.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunSurfacesDirectoryCleanupFailureAfterDurableSuccess()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var root = Path.Combine(Path.GetTempPath(), $"oc-lost-ack-locked-{Guid.NewGuid():N}");
        FileStream? sentinel = null;
        try
        {
            Exception? observed = null;
            try
            {
                _ = await DurableHttpLostAckScenario.RunWithRootFactoryAsync(
                    () =>
                    {
                        _ = Directory.CreateDirectory(root);
                        sentinel = new(Path.Combine(root, "sentinel.lock"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                        return root;
                    },
                    CancellationToken.None);
            }
            catch (Exception exception)
            {
                observed = exception;
            }

            await Assert.That(observed).IsTypeOf<InvalidOperationException>();
            await Assert.That(observed?.InnerException).IsTypeOf<IOException>();
            await Assert.That(Directory.Exists(root)).IsTrue();
        }
        finally
        {
            if (sentinel is not null)
            {
                await sentinel.DisposeAsync();
            }

            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    /// <summary>Run cleanup keeps both a primary workflow failure and cleanup failure visible.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunFailureCompositionPreservesBothCauses()
    {
        DurableHttpLostAckScenario.ThrowIfRunFailed(null, null);
        var primary = new InvalidOperationException("workflow failed");
        var cleanup = new IOException("cleanup failed");

        await Assert.That(() => DurableHttpLostAckScenario.ThrowIfRunFailed(primary, null)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => DurableHttpLostAckScenario.ThrowIfRunFailed(primary, cleanup)).ThrowsExactly<AggregateException>();
    }

    /// <summary>Async cleanup aggregates two independent failures after attempting both operations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AsyncCleanupAccumulatorsPreserveEveryFailure()
    {
        var first = new InvalidOperationException("first");
        var second = new IOException("second");
        var run = await DurableHttpLostAckScenario.CaptureRunCleanupFailureAsync(null, () => Task.FromException(first));
        run = await DurableHttpLostAckScenario.CaptureRunCleanupFailureAsync(run, () => Task.FromException(second));
        var host = await DurableHttpLostAckScenario.DurableHttpLostAckHost.CaptureCleanupFailureAsync(null, () => Task.FromException(first));
        host = await DurableHttpLostAckScenario.DurableHttpLostAckHost.CaptureCleanupFailureAsync(host, () => Task.FromException(second));
        var session = await DurableHttpLostAckScenario.CaptureSessionCleanupFailureAsync(null, () => Task.FromException(first));
        session = await DurableHttpLostAckScenario.CaptureSessionCleanupFailureAsync(session, () => Task.FromException(second));

        await Assert.That((run as AggregateException)?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
        await Assert.That((host as AggregateException)?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
        await Assert.That((session as AggregateException)?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
        await Assert.That(await DurableHttpLostAckScenario.CaptureRunCleanupFailureAsync(run, static () => Task.CompletedTask)).IsSameReferenceAs(run);
        await Assert.That(await DurableHttpLostAckScenario.DurableHttpLostAckHost.CaptureCleanupFailureAsync(host, static () => Task.CompletedTask)).IsSameReferenceAs(host);
        await Assert.That(await DurableHttpLostAckScenario.CaptureSessionCleanupFailureAsync(session, static () => Task.CompletedTask)).IsSameReferenceAs(session);
    }

    /// <summary>Sync cleanup retains earlier failures and returns the first failure from a failing action.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SyncCleanupAccumulatorPreservesEveryFailure()
    {
        var first = new InvalidOperationException("first");
        var second = new IOException("second");
        var result = DurableHttpLostAckScenario.CaptureRunCleanupFailure(null, () => throw first);
        result = DurableHttpLostAckScenario.CaptureRunCleanupFailure(result, () => throw second);
        await Assert.That((result as AggregateException)?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
        await Assert.That(DurableHttpLostAckScenario.CaptureRunCleanupFailure(result, static () => { })).IsSameReferenceAs(result);
    }

    /// <summary>HTTP client cleanup leaves a preexisting failure untouched when no client was created.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HttpClientCleanupRetainsExistingFailureForMissingClient()
    {
        var existing = new InvalidOperationException("earlier cleanup failed");
        await Assert.That(DurableHttpLostAckScenario.CaptureHttpClientCleanupFailure(existing, null)).IsSameReferenceAs(existing);
    }

    /// <summary>HTTP client cleanup attempts disposal and combines a disposal fault with an earlier failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HttpClientCleanupSurfacesDisposalFault()
    {
        var successful = new DisposalProbeHttpClient(throwOnDispose: false);
        await Assert.That(DurableHttpLostAckScenario.CaptureHttpClientCleanupFailure(null, successful)).IsNull();
        await Assert.That(successful.DisposeCount).IsEqualTo(1);

        var firstFailure = DurableHttpLostAckScenario.CaptureHttpClientCleanupFailure(null, new DisposalProbeHttpClient(throwOnDispose: true));
        await Assert.That(firstFailure).IsTypeOf<IOException>();
        var earlier = new InvalidOperationException("earlier failure");
        var combined = DurableHttpLostAckScenario.CaptureHttpClientCleanupFailure(earlier, new DisposalProbeHttpClient(throwOnDispose: true));
        await Assert.That((combined as AggregateException)?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
    }

    /// <summary>Partial cleanup respects context ownership and accepts an empty resource set.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PartialSessionCleanupRespectsContextOwnership()
    {
        var context = new AsyncDisposalProbe(null);
        var transport = new AsyncDisposalProbe(null);
        var store = new AsyncDisposalProbe(null);
        await DurableHttpLostAckScenario.DisposeFailedSessionAsync(context, transport, store);
        await Assert.That(context.DisposeCount).IsEqualTo(1);
        await Assert.That(transport.DisposeCount).IsEqualTo(0);
        await Assert.That(store.DisposeCount).IsEqualTo(0);
        await DurableHttpLostAckScenario.DisposeFailedSessionAsync(null, null, null);
    }

    /// <summary>Partial cleanup attempts both independently owned resources and surfaces every disposal fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PartialSessionCleanupPreservesMultipleDisposalFailures()
    {
        var transportFailure = new IOException("transport cleanup failed");
        var storeFailure = new InvalidOperationException("store cleanup failed");
        var transport = new AsyncDisposalProbe(transportFailure);
        var store = new AsyncDisposalProbe(storeFailure);
        Exception? observed = null;
        try
        {
            await DurableHttpLostAckScenario.DisposeFailedSessionAsync(null, transport, store);
        }
        catch (Exception exception)
        {
            observed = exception;
        }

        var aggregate = (observed as InvalidOperationException)?.InnerException as AggregateException;
        await Assert.That(aggregate?.InnerExceptions.Count).IsEqualTo(CombinedFailureCount);
        await Assert.That(aggregate?.InnerExceptions[0]).IsSameReferenceAs(transportFailure);
        await Assert.That(aggregate?.InnerExceptions[1]).IsSameReferenceAs(storeFailure);
        await Assert.That(transport.DisposeCount).IsEqualTo(1);
        await Assert.That(store.DisposeCount).IsEqualTo(1);

        var context = new AsyncDisposalProbe(transportFailure);
        await Assert.That(async () => await DurableHttpLostAckScenario.DisposeFailedSessionAsync(context, transport, store)).Throws<InvalidOperationException>();
        await Assert.That(context.DisposeCount).IsEqualTo(1);
        await Assert.That(transport.DisposeCount).IsEqualTo(1);
        await Assert.That(store.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Counts disposal and optionally faults after releasing the base client resources.</summary>
    /// <param name="throwOnDispose">Whether disposal should fail.</param>
    private sealed class DisposalProbeHttpClient(bool throwOnDispose) : HttpClient
    {
        /// <summary>Gets the number of dispose calls.</summary>
        internal int DisposeCount { get; private set; }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            DisposeCount++;
            if (throwOnDispose)
            {
                throw new IOException("HTTP client disposal failed.");
            }
        }
    }

    /// <summary>Observes asynchronous disposal and can produce a real disposal fault.</summary>
    /// <param name="failure">The failure to throw after the disposal boundary.</param>
    private sealed class AsyncDisposalProbe(Exception? failure) : IAsyncDisposable
    {
        /// <summary>Gets the number of disposal attempts.</summary>
        internal int DisposeCount { get; private set; }

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => PerformDisposeAsync();

        /// <summary>Performs the disposal attempt, preserving an injected collaborator failure.</summary>
        /// <returns>The disposal task.</returns>
        /// <exception cref="Exception">The injected disposal failure.</exception>
        private async ValueTask PerformDisposeAsync()
        {
            DisposeCount++;
            await Task.Yield();
            if (failure is not null)
            {
                throw failure;
            }
        }
    }
}
