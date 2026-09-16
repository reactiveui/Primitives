// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplayProtectionOptions"/>.</summary>
public sealed class HttpReplayProtectionOptionsTests
{
    /// <summary>The default replay entry count.</summary>
    private const int DefaultMaximumEntries = 8192;

    /// <summary>The default replay waiter count.</summary>
    private const int DefaultMaximumActiveReplayWaiters = 1024;

    /// <summary>The default replay session count.</summary>
    private const int DefaultMaximumReplaySessions = 4096;

    /// <summary>The number of bytes in one kibibyte.</summary>
    private const long BytesPerKilobyte = 1024;

    /// <summary>The number of kibibytes in one mebibyte.</summary>
    private const long KibibytesPerMebibyte = 1024;

    /// <summary>The default retained mebibyte limit.</summary>
    private const long DefaultRetainedMebibytes = 8;

    /// <summary>The default freshness minutes.</summary>
    private const int DefaultFreshnessMinutes = 5;

    /// <summary>The default nonce retention minutes.</summary>
    private const int DefaultNonceRetentionMinutes = 10;

    /// <summary>The default replay session retention minutes.</summary>
    private const int DefaultSessionRetentionMinutes = 30;

    /// <summary>The shorter nonce retention minutes.</summary>
    private const int ShortNonceRetentionMinutes = 4;

    /// <summary>The long nonce retention minutes.</summary>
    private const int LongNonceRetentionMinutes = 12;

    /// <summary>The short replay session retention minutes.</summary>
    private const int ShortSessionRetentionMinutes = 11;

    /// <summary>The zero tick invalid window.</summary>
    private const int ZeroTicks = 0;

    /// <summary>The negative tick invalid window.</summary>
    private const int NegativeTicks = -1;

    /// <summary>The multiplier used for the future-skew retention invariant.</summary>
    private const int FutureSkewRetentionMultiplier = 2;

    /// <summary>The entries option selector.</summary>
    private const string EntriesField = "entries";

    /// <summary>The waiters option selector.</summary>
    private const string WaitersField = "waiters";

    /// <summary>The sessions option selector.</summary>
    private const string SessionsField = "sessions";

    /// <summary>The bytes option selector.</summary>
    private const string BytesField = "bytes";

    /// <summary>The canonical bytes option selector.</summary>
    private const string CanonicalField = "canonical";

    /// <summary>The response bytes option selector.</summary>
    private const string ResponseField = "response";

    /// <summary>The default retained byte limit.</summary>
    private const long DefaultMaximumRetainedBytes = DefaultRetainedMebibytes * BytesPerKilobyte * KibibytesPerMebibyte;

    /// <summary>Verifies the defaults match the accepted replay bounds.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DefaultsMatchAcceptedReplayBounds()
    {
        var options = new HttpReplayProtectionOptions();

        await Assert.That(options.Enabled).IsTrue();
        await Assert.That(options.TimeProvider).IsSameReferenceAs(TimeProvider.System);
        await Assert.That(options.MaximumEntries).IsEqualTo(DefaultMaximumEntries);
        await Assert.That(options.MaximumActiveReplayWaiters).IsEqualTo(DefaultMaximumActiveReplayWaiters);
        await Assert.That(options.MaximumReplaySessions).IsEqualTo(DefaultMaximumReplaySessions);
        await Assert.That(options.MaximumRetainedBytes).IsEqualTo(DefaultMaximumRetainedBytes);
        await Assert.That(options.FreshnessWindow).IsEqualTo(TimeSpan.FromMinutes(DefaultFreshnessMinutes));
        await Assert.That(options.NonceRetention).IsEqualTo(TimeSpan.FromMinutes(DefaultNonceRetentionMinutes));
        await Assert.That(options.ReplaySessionRetention).IsEqualTo(TimeSpan.FromMinutes(DefaultSessionRetentionMinutes));
    }

    /// <summary>Verifies every positive numeric replay bound is validated.</summary>
    /// <param name="field">The configured field to invalidate.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(EntriesField)]
    [Arguments(WaitersField)]
    [Arguments(SessionsField)]
    [Arguments(BytesField)]
    [Arguments(CanonicalField)]
    [Arguments(ResponseField)]
    public async Task ValidateRejectsNonPositiveLimits(string field)
    {
        var options = field switch
        {
            EntriesField => new HttpReplayProtectionOptions { MaximumEntries = ZeroTicks },
            WaitersField => new HttpReplayProtectionOptions { MaximumActiveReplayWaiters = ZeroTicks },
            SessionsField => new HttpReplayProtectionOptions { MaximumReplaySessions = ZeroTicks },
            BytesField => new HttpReplayProtectionOptions { MaximumRetainedBytes = ZeroTicks },
            CanonicalField => new HttpReplayProtectionOptions { MaximumCanonicalRequestBytes = ZeroTicks },
            _ => new HttpReplayProtectionOptions { MaximumCachedResponseBytes = ZeroTicks },
        };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies replay time windows must be positive and finite.</summary>
    /// <param name="window">The invalid window.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(ZeroTicks)]
    [Arguments(NegativeTicks)]
    public async Task ValidateRejectsNonPositiveTimeWindows(int window)
    {
        var options = new HttpReplayProtectionOptions { FreshnessWindow = TimeSpan.FromTicks(window) };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies freshness cannot overflow while deriving the future-skew session retention bound.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateRejectsFreshnessWindowTooLargeToDouble()
    {
        var hugeWindow = TimeSpan.FromTicks((TimeSpan.MaxValue.Ticks / FutureSkewRetentionMultiplier) + 1);
        var coveringWindow = TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks - 1);
        var options = new HttpReplayProtectionOptions { FreshnessWindow = hugeWindow, NonceRetention = coveringWindow, ReplaySessionRetention = coveringWindow };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies nonce retention must cover freshness.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateRequiresNonceRetentionToCoverFreshness()
    {
        var options = new HttpReplayProtectionOptions { FreshnessWindow = TimeSpan.FromMinutes(DefaultFreshnessMinutes), NonceRetention = TimeSpan.FromMinutes(ShortNonceRetentionMinutes) };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies replay sessions cover nonce retention and two freshness windows.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateRequiresSessionRetentionToCoverNonceAndFutureSkew()
    {
        var options = new HttpReplayProtectionOptions
        {
            FreshnessWindow = TimeSpan.FromMinutes(DefaultFreshnessMinutes),
            NonceRetention = TimeSpan.FromMinutes(LongNonceRetentionMinutes),
            ReplaySessionRetention = TimeSpan.FromMinutes(ShortSessionRetentionMinutes),
        };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
