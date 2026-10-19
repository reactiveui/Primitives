// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests failed initialization and cleanup of shared engine resources.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a failed store initialization is shared without opening transport or retrying partial initialization.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FailedStoreInitializationIsSharedAcrossStartupAndSubscriptionRequests()
    {
        var store = new RecordingStore { OnInitialize = static () => throw new IOException("Store could not be opened.") };
        var transport = new RecordingTransport();
        await using var engine = CreateEngine(store, transport);

        await Assert.That(async () => await engine.StartAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<IOException>();
        await Assert.That(async () => await engine.EnsureSubscriptionIdAsync(Stream, Subscription, CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<IOException>();
        await Assert.That(async () => await engine.StartAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<IOException>();

        await Assert.That(store.InitializeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(0);
    }

    /// <summary>Verifies a failed session close does not prevent owned dependency disposal or repeat session cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncClosesOwnedDependenciesWhenSessionCloseFails()
    {
        var session = new ReceiveSession { DisposeException = new IOException("Session close failed.") };
        var transport = new RecordingTransport { SessionOverride = session };
        var store = new RecordingStore();
        var engine = CreateEngine(store, transport);
        await engine.StartAsync(CancellationToken.None);

        await Assert.That(async () => await engine.DisposeAsync().ConfigureAwait(false))
            .ThrowsExactly<IOException>();
        await Assert.That(async () => await engine.DisposeAsync().ConfigureAwait(false))
            .ThrowsExactly<IOException>();

        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }
}
