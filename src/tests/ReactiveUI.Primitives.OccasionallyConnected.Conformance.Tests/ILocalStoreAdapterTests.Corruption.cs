// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <content>Fail-closed corruption checks for native database stores.</content>
public sealed partial class ILocalStoreAdapterTests
{
    /// <summary>The native header bytes corrupted after a complete acknowledged commit.</summary>
    private const int CorruptedHeaderBytes = 256;

    /// <summary>The invalid header byte pattern.</summary>
    private const byte InvalidHeaderByte = 0xA5;

    /// <summary>Checks corruption cannot reopen as a successful empty store.</summary>
    /// <param name="provider">The native database provider.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments(1)]
    [Arguments(LiteDbProvider)]
    [Arguments(BliteDbProvider)]
    [Arguments(EncryptedSqliteProvider)]
    public async Task CorruptDatabaseCannotSilentlyResetCommittedState(int provider)
    {
        await using var fixture = new StoreFixture(provider);
        await using (var store = await fixture.OpenAsync())
        {
            _ = await LocalStoreConformance.SeedDurableAsync(store);
        }

        var database = Path.Combine(fixture.DirectoryPath, "store.db");
        await using (var corrupt = new FileStream(database, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            var bytes = new byte[CorruptedHeaderBytes];
            Array.Fill(bytes, InvalidHeaderByte);
            await corrupt.WriteAsync(bytes);
            await corrupt.FlushAsync();
        }

        Func<Task> recover = async () =>
        {
            await using var reopened = await fixture.OpenAsync();
            var subscription = await reopened.GetOrCreateSubscriptionIdAsync(LocalStoreConformance.Stream, null, CancellationToken.None);
            _ = await reopened.RecoverStreamAsync(LocalStoreConformance.Stream, subscription, CancellationToken.None);
        };
        _ = await Assert.ThrowsAsync<Exception>(recover);
    }
}
