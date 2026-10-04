// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests configuration rejection before engine dependency ownership transfers.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies malformed configuration does not initialize or dispose caller dependencies.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvalidConfigurationDoesNotConsumeCallerDependencies()
    {
        await using var store = new RecordingStore();
        await using var transport = new RecordingTransport();
        var valid = new SyncEngineOptions
        {
            Store = store,
            Transport = transport,
            StoreInitialization = new("configuration-tests", 1, false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
        };
        SyncEngineOptions[] invalid =
        [
            valid with { Store = null! },
            valid with { Transport = null! },
            valid with { TimeProvider = null! },
            valid with { Options = null! },
            valid with { StoreInitialization = null! },
            valid with { Client = null! },
            valid with { SupportedProtocolVersions = null! },
            valid with { StoreOwnership = (SyncEngineDependencyOwnership)(-1) },
            valid with { TransportOwnership = (SyncEngineDependencyOwnership)(-1) },
            valid with { MaxRegisteredStreams = 0 },
            valid with { MaxActiveSubscriptions = 0 },
            valid with { MaxDiagnosticSubscriptions = 0 },
            valid with { MaxSchedulerDescriptorBytes = 0 },
            valid with { SupportedProtocolVersions = new(new(2, 0), new(1, 0)) },
        ];

        foreach (var options in invalid)
        {
            await Assert.That(() => new SyncEngine(options)).ThrowsExactly<InvalidOperationException>();
        }

        await Assert.That(store.InitializeCalls).IsEqualTo(0);
        await Assert.That(store.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.ConnectCalls).IsEqualTo(0);
        await Assert.That(transport.DisposeCalls).IsEqualTo(0);
        await using var engine = new SyncEngine(valid);
        await engine.StartAsync(CancellationToken.None);
        await Assert.That(store.InitializeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
    }
}
