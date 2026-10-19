// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests parsing HTTP Retry-After lower-bound hints.</summary>
public sealed class HttpRetryAfterParserTests
{
    /// <summary>The valid RFC 9110 delay-seconds value used by the tests.</summary>
    private const int DelaySeconds = 120;

    /// <summary>The delay represented by <see cref="RetryAfterDate"/> from the observed test time.</summary>
    private const int DateDelaySeconds = 37;

    /// <summary>The largest delay-seconds value accepted by the platform typed parser.</summary>
    private const int MaximumTypedDelaySeconds = int.MaxValue;

    /// <summary>The raw header length that exceeds the parser's maximum.</summary>
    private const int OversizedHeaderLength = 129;

    /// <summary>The raw HTTP-date used for date parsing tests.</summary>
    private const string RetryAfterDate = "Sun, 06 Nov 1994 08:49:37 GMT";

    /// <summary>The raw past HTTP-date used for date parsing tests.</summary>
    private const string PastRetryAfterDate = "Thu, 01 Jan 1970 00:00:00 GMT";

    /// <summary>A raw header that exceeds the parser's maximum length.</summary>
    private static readonly string OversizedRetryAfter = new('1', OversizedHeaderLength);

    /// <summary>Verifies delay-seconds are returned as server lower bounds.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DelaySecondsReturnsServerLowerBound()
    {
        var delay = HttpRetryAfterParser.Parse(DelaySeconds.ToString(System.Globalization.CultureInfo.InvariantCulture), DateTimeOffset.UnixEpoch);

        await Assert.That(delay).IsEqualTo(TimeSpan.FromSeconds(DelaySeconds));
    }

    /// <summary>Verifies a valid HTTP-date containing a comma is not treated as multiple values.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HttpDateReturnsDelayFromObservedTime()
    {
        var observedUtc = DateTimeOffset.Parse("Sun, 06 Nov 1994 08:49:00 GMT", System.Globalization.CultureInfo.InvariantCulture);

        var delay = HttpRetryAfterParser.Parse(RetryAfterDate, observedUtc);

        await Assert.That(delay).IsEqualTo(TimeSpan.FromSeconds(DateDelaySeconds));
    }

    /// <summary>Verifies a past HTTP-date returns a zero lower bound.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PastHttpDateReturnsZero()
    {
        var delay = HttpRetryAfterParser.Parse(PastRetryAfterDate, DateTimeOffset.UnixEpoch.AddTicks(1));

        await Assert.That(delay).IsEqualTo(TimeSpan.Zero);
    }

    /// <summary>Verifies absent, malformed, nonconforming, and multiple header values are rejected.</summary>
    /// <param name="rawRetryAfter">The malformed raw header value.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments("-1")]
    [Arguments("1.5")]
    [Arguments("2147483648")]
    [Arguments("120, 121")]
    [Arguments("not-a-retry-hint")]
    [Arguments("+1")]
    [Arguments("120\r\n ")]
    [Arguments("120\n")]
    [Arguments("Sun, 06 Nov 1994 08:49:37 GMT, 120")]
    public async Task InvalidValuesReturnNull(string? rawRetryAfter)
    {
        var delay = HttpRetryAfterParser.Parse(rawRetryAfter, DateTimeOffset.UnixEpoch);

        await Assert.That(delay).IsNull();
    }

    /// <summary>Verifies raw input beyond the parser bound is rejected before typed parsing.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task OversizedValueReturnsNull()
    {
        var delay = HttpRetryAfterParser.Parse(OversizedRetryAfter, DateTimeOffset.UnixEpoch);

        await Assert.That(delay).IsNull();
    }

    /// <summary>Verifies the upper platform-representable delay-seconds value is retained.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task MaximumTypedDelaySecondsIsRetained()
    {
        var delay = HttpRetryAfterParser.Parse(MaximumTypedDelaySeconds.ToString(System.Globalization.CultureInfo.InvariantCulture), DateTimeOffset.UnixEpoch);

        await Assert.That(delay).IsEqualTo(TimeSpan.FromSeconds(MaximumTypedDelaySeconds));
    }

    /// <summary>Verifies the raw length limit with values that otherwise parse successfully.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ExactHeaderLengthAcceptsZeroButOneExtraSpaceRejectsIt()
    {
        const int acceptedPaddingLength = 127;
        var accepted = $"{new string(' ', acceptedPaddingLength)}0";
        var rejected = $" {accepted}";

        await Assert.That(HttpRetryAfterParser.Parse(accepted, DateTimeOffset.UnixEpoch)).IsEqualTo(TimeSpan.Zero);
        await Assert.That(HttpRetryAfterParser.Parse(rejected, DateTimeOffset.UnixEpoch)).IsNull();
    }

    /// <summary>Verifies offset representation does not change the observed instant used for a date hint.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EqualHttpDateReturnsZeroForAnOffsetObservedInstant()
    {
        var observed = DateTimeOffset.UnixEpoch.ToOffset(TimeSpan.FromHours(1));

        await Assert.That(HttpRetryAfterParser.Parse(PastRetryAfterDate, observed)).IsEqualTo(TimeSpan.Zero);
    }
}
