// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="StaticLocalStoreKeyProvider"/>.</summary>
public sealed class StaticLocalStoreKeyProviderTests
{
    /// <summary>The current key identifier.</summary>
    private const string CurrentKeyId = "current";

    /// <summary>Verifies the static provider exposes the current key and resolves retired keys by identifier.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StaticProviderResolvesCurrentAndRetiredKeys()
    {
        var current = LocalStoreKey.CreateRandom(CurrentKeyId);
        var retired = LocalStoreKey.CreateRandom("retired");
        var provider = new StaticLocalStoreKeyProvider(current, [retired]);

        await Assert.That(provider.GetCurrentKey()).IsSameReferenceAs(current);
        await Assert.That(provider.GetKey("retired")).IsSameReferenceAs(retired);
        await Assert.That(provider.GetKey(CurrentKeyId)).IsSameReferenceAs(current);
        await Assert.That(provider.GetKey("unknown")).IsNull();
        await Assert.That(() => new StaticLocalStoreKeyProvider(current, [LocalStoreKey.CreateRandom(CurrentKeyId)]))
            .ThrowsExactly<ArgumentException>();
    }
}
