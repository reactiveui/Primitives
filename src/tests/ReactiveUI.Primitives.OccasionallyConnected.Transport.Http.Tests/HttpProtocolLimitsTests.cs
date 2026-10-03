// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolLimits"/>.</summary>
public sealed class HttpProtocolLimitsTests
{
    /// <summary>The test kilobyte size.</summary>
    private const int TestKilobyte = 1024;

    /// <summary>The default message size used by limits tests.</summary>
    private const int TestMessageKilobytes = 32;

    /// <summary>The default payload size used by limits tests.</summary>
    private const int TestPayloadKilobytes = 8;

    /// <summary>The default collection size used by limits tests.</summary>
    private const int TestMaximumCollectionCount = 8;

    /// <summary>The metadata key byte limit used by limits tests.</summary>
    private const int TestMetadataKeyBytes = 64;

    /// <summary>The metadata value byte limit used by limits tests.</summary>
    private const int TestMetadataValueBytes = 256;

    /// <summary>The JSON depth used by limits tests.</summary>
    private const int TestJsonDepth = 32;

    /// <summary>Verifies protocol limits complete explicit values and reject invalid configuration.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompletePreservesExplicitDerivedLimitsAndRejectsInvalidValues()
    {
        var defaults = CreateLimits().Complete();
        var explicitLimits = CreateLimits() with
        {
            MaximumQueryBytes = TestKilobyte,
            MaximumProtocolStringBytes = TestKilobyte,
        };
        var completed = explicitLimits.Complete();

        await Assert.That(defaults.MaximumQueryBytes).IsEqualTo(defaults.MaximumRequestBytes);
        await Assert.That(defaults.MaximumProtocolStringBytes).IsLessThanOrEqualTo(TestMessageKilobytes * TestKilobyte);
        await Assert.That(completed.MaximumQueryBytes).IsEqualTo(TestKilobyte);
        await Assert.That(completed.MaximumProtocolStringBytes).IsEqualTo(TestKilobyte);
        await Assert.That(static () => (CreateLimits() with { MaximumJsonDepth = 0 }).Complete())
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Creates protocol limits for direct limit tests.</summary>
    /// <returns>The limits.</returns>
    private static HttpProtocolLimits CreateLimits() => new()
    {
        MaximumRequestBytes = TestMessageKilobytes * TestKilobyte,
        MaximumResponseBytes = TestMessageKilobytes * TestKilobyte,
        MaximumPayloadBytes = TestPayloadKilobytes * TestKilobyte,
        MaximumMetadataEntries = TestMaximumCollectionCount,
        MaximumMetadataKeyBytes = TestMetadataKeyBytes,
        MaximumMetadataValueBytes = TestMetadataValueBytes,
        MaximumBatchOperations = TestMaximumCollectionCount,
        MaximumEventsPerBatch = TestMaximumCollectionCount,
        MaximumCompletedOperationsPerBatch = TestMaximumCollectionCount,
        MaximumJsonDepth = TestJsonDepth,
    };
}
