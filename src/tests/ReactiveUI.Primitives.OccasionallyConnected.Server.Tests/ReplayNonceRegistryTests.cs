// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ReplayNonceRegistry"/>.</summary>
public sealed class ReplayNonceRegistryTests
{
    /// <summary>The stable authenticated tenant used by most test requests.</summary>
    private const string Tenant = "tenant";

    /// <summary>The alternate authenticated tenant used to prove scope isolation.</summary>
    private const string AlternateTenant = "tenant-b";

    /// <summary>The stable authenticated client used by most test requests.</summary>
    private const string Client = "client";

    /// <summary>The alternate authenticated client used to prove scope isolation.</summary>
    private const string AlternateClient = "client-b";

    /// <summary>The stable nonce used by most test requests.</summary>
    private const string Nonce = "nonce";

    /// <summary>The nonce used for past retention checks.</summary>
    private const string ExpiredNonce = "expired";

    /// <summary>The nonce used for current retention checks.</summary>
    private const string CurrentNonce = "current";

    /// <summary>The nonce used for count capacity checks.</summary>
    private const string FirstNonce = "first";

    /// <summary>The nonce used for count capacity rejection checks.</summary>
    private const string SecondNonce = "second";

    /// <summary>The blank key used for invalid identifier checks.</summary>
    private const string BlankKey = " ";

    /// <summary>The single-character tenant used for exact retained-byte accounting.</summary>
    private const string ShortTenant = "t";

    /// <summary>The single-character client used for exact retained-byte accounting.</summary>
    private const string ShortClient = "c";

    /// <summary>The single-character nonce used for exact retained-byte accounting.</summary>
    private const string ShortNonce = "n";

    /// <summary>A different nonce with the same encoded byte count.</summary>
    private const string AlternateShortNonce = "m";

    /// <summary>The nonce used for the past boundary freshness test.</summary>
    private const string PastNonce = "past";

    /// <summary>The nonce used for the future boundary freshness test.</summary>
    private const string FutureNonce = "future";

    /// <summary>The nonce used for the empty request body test.</summary>
    private const string EmptyNonce = "empty";

    /// <summary>The nonce used for the oversized request body test.</summary>
    private const string LargeNonce = "large";

    /// <summary>The first byte in the default canonical request.</summary>
    private const byte RequestFirstByte = 1;

    /// <summary>The second byte in the default canonical request.</summary>
    private const byte RequestSecondByte = 2;

    /// <summary>The third byte in the default canonical request.</summary>
    private const byte RequestThirdByte = 3;

    /// <summary>The byte used after mutating a previously hashed caller buffer.</summary>
    private const byte MutatedRequestByte = 4;

    /// <summary>The first byte in the oversized request sample.</summary>
    private const byte OversizedRequestFirstByte = 5;

    /// <summary>The second byte in the oversized request sample.</summary>
    private const byte OversizedRequestSecondByte = 6;

    /// <summary>The invalid high-surrogate character used for strict UTF-8 checks.</summary>
    private const char MalformedSurrogate = '\ud800';

    /// <summary>The invalid low-surrogate character used for strict UTF-8 checks.</summary>
    private const char MalformedLowSurrogate = '\udc00';

    /// <summary>The Unicode scalar used to verify valid surrogate pairs are accepted.</summary>
    private const int ValidSupplementaryCodePoint = 128_512;

    /// <summary>The number of characters in an intentionally malformed UTF-16 key.</summary>
    private const int SingleCharacter = 1;

    /// <summary>The length just beyond the authenticated identity limit.</summary>
    private const int OversizedIdentityLength = 257;

    /// <summary>The length just beyond the nonce limit.</summary>
    private const int OversizedNonceLength = 129;

    /// <summary>The default retained entry limit used by tests.</summary>
    private const int DefaultMaximumEntries = 8;

    /// <summary>The default canonical request size limit used by tests.</summary>
    private const int DefaultMaximumRequestBytes = 32;

    /// <summary>The default retained metadata byte limit used by tests.</summary>
    private const long DefaultMaximumRetainedBytes = 4096;

    /// <summary>The retained byte count for a one-character tenant, client and nonce.</summary>
    private const long ShortRequestRetainedBytes = 51;

    /// <summary>The retained byte limit just below one short request.</summary>
    private const long TooSmallRetainedByteLimit = ShortRequestRetainedBytes - 1;

    /// <summary>The retained byte limit used for constructor validation.</summary>
    private const long ConstructorRetainedBytes = 128;

    /// <summary>The large canonical request length used to prove bodies are not retained.</summary>
    private const int LargeRequestBytes = 1024;

    /// <summary>The number of concurrent callers in the thread-safety test.</summary>
    private const int ConcurrentCallCount = 32;

    /// <summary>The retained entry count expected after three authenticated scopes use the same nonce.</summary>
    private const int IsolatedScopeCount = 3;

    /// <summary>The baseline clock instant used by the registry tests.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 12, 10, 0, 0, TimeSpan.Zero);

    /// <summary>The accepted request freshness window used by the registry tests.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(5);

    /// <summary>The finite guard for a cross-thread clock callback.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies configured nonce retention still prevents changed requests after freshness expires.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConfiguredRetentionPreventsNonceReuseWithFreshTimestamp()
    {
        var clock = new ManualTimeProvider(Start);
        var registry = CreateRegistry(clock, nonceRetention: Window + Window);
        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, CreateRequest())).IsFalse();

        var later = Start.Add(Window).AddTicks(1);
        clock.SetUtcNow(later);
        await Assert.That(() => registry.IsReplay(Tenant, Client, Nonce, later, CreateRequest()))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(registry.Count).IsEqualTo(1);
    }

    /// <summary>Verifies a future timestamp remains protected through its full accepted freshness interval.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FutureTimestampRemainsProtectedPastMinimumRetention()
    {
        var clock = new ManualTimeProvider(Start);
        var registry = CreateRegistry(clock);
        var future = Start.Add(Window);
        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, future, CreateRequest())).IsFalse();

        clock.SetUtcNow(future.Add(Window));
        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, future, CreateRequest())).IsTrue();
        await Assert.That(registry.Count).IsEqualTo(1);
    }

    /// <summary>Verifies exact byte capacity preserves a retained nonce until expiry and then reclaims its bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ByteCapacityIsReclaimedOnlyAfterNonceExpiry()
    {
        var clock = new ManualTimeProvider(Start);
        var registry = CreateRegistry(clock, maximumRetainedBytes: ShortRequestRetainedBytes);
        await Assert.That(registry.IsReplay(ShortTenant, ShortClient, ShortNonce, Start, CreateRequest())).IsFalse();
        await Assert.That(() => registry.IsReplay(ShortTenant, ShortClient, AlternateShortNonce, Start, CreateRequest()))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(registry.IsReplay(ShortTenant, ShortClient, ShortNonce, Start, CreateRequest())).IsTrue();

        var later = Start.Add(Window).AddTicks(1);
        clock.SetUtcNow(later);
        await Assert.That(registry.IsReplay(ShortTenant, ShortClient, AlternateShortNonce, later, CreateRequest())).IsFalse();
        await Assert.That(registry.Count).IsEqualTo(1);
        await Assert.That(registry.RetainedBytes).IsEqualTo(ShortRequestRetainedBytes);
    }

    /// <summary>Verifies identical authenticated retries are recognized after the first admission.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenIdenticalAuthenticatedRequestRepeats_ThenItIsRecognizedAsReplay()
    {
        var registry = CreateRegistry();
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, request)).IsFalse();
        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, request)).IsTrue();
        await Assert.That(registry.Count).IsEqualTo(1);
    }

    /// <summary>Verifies the authenticated tenant and client isolate identical nonce values.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenSameNonceUsesDifferentAuthenticatedScope_ThenRequestsAreIndependent()
    {
        var registry = CreateRegistry();
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, request)).IsFalse();
        await Assert.That(registry.IsReplay(AlternateTenant, Client, Nonce, Start, request)).IsFalse();
        await Assert.That(registry.IsReplay(Tenant, AlternateClient, Nonce, Start, request)).IsFalse();
        await Assert.That(registry.Count).IsEqualTo(IsolatedScopeCount);
    }

    /// <summary>Verifies nonce reuse cannot change the canonical request bytes.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenNonceReuseChangesPayload_ThenItIsRejected()
    {
        var registry = CreateRegistry();
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, request)).IsFalse();
        request[0] = MutatedRequestByte;

        await Assert
            .That(() => registry.IsReplay(Tenant, Client, Nonce, Start, request))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies nonce reuse cannot change the authenticated timestamp.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenNonceReuseChangesTimestamp_ThenItIsRejected()
    {
        var registry = CreateRegistry();
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, request)).IsFalse();

        await Assert
            .That(() => registry.IsReplay(Tenant, Client, Nonce, Start.AddTicks(1), request))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies requests are accepted at both freshness boundaries.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestTimestampIsAtFreshnessBoundary_ThenItIsAccepted()
    {
        var registry = CreateRegistry();
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, PastNonce, Start.Subtract(Window), request)).IsFalse();
        await Assert.That(registry.IsReplay(Tenant, Client, FutureNonce, Start.Add(Window), request)).IsFalse();
    }

    /// <summary>Verifies requests older than the accepted freshness window are rejected.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestTimestampIsTooOld_ThenItIsRejected()
    {
        var registry = CreateRegistry();
        var request = CreateRequest();

        await Assert
            .That(() => registry.IsReplay(Tenant, Client, Nonce, Start.Subtract(Window).AddTicks(-1), request))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies requests newer than the accepted freshness window are rejected.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestTimestampIsTooFarInFuture_ThenItIsRejected()
    {
        var registry = CreateRegistry();
        var request = CreateRequest();

        await Assert
            .That(() => registry.IsReplay(Tenant, Client, Nonce, Start.Add(Window).AddTicks(1), request))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies retention keeps entries through their inclusive expiry instant.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRetentionReachesExpiryInstant_ThenEntryIsStillRetained()
    {
        var clock = new ManualTimeProvider(Start);
        var registry = CreateRegistry(clock);
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, request)).IsFalse();
        clock.SetUtcNow(Start.Add(Window));

        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, Start, request)).IsTrue();
        await Assert.That(registry.Count).IsEqualTo(1);
    }

    /// <summary>Verifies expired entries are pruned when a later valid request arrives.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRetentionPassesExpiryInstant_ThenExpiredEntriesArePruned()
    {
        var clock = new ManualTimeProvider(Start);
        var registry = CreateRegistry(clock);
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, ExpiredNonce, Start, request)).IsFalse();
        clock.SetUtcNow(Start.Add(Window).AddTicks(1));

        await Assert.That(registry.IsReplay(Tenant, Client, CurrentNonce, clock.GetUtcNow(), request)).IsFalse();
        await Assert.That(registry.Count).IsEqualTo(1);
    }

    /// <summary>Verifies the latest observed server clock prevents rollback from reopening a pruned request.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenClockRollsBackAfterPrune_ThenExpiredRequestDoesNotReopen()
    {
        var clock = new ManualTimeProvider(Start);
        var registry = CreateRegistry(clock);
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, ExpiredNonce, Start, request)).IsFalse();
        clock.SetUtcNow(Start.Add(Window).AddTicks(1));
        await Assert.That(registry.IsReplay(Tenant, Client, CurrentNonce, clock.GetUtcNow(), request)).IsFalse();
        clock.SetUtcNow(Start);

        await Assert
            .That(() => registry.IsReplay(Tenant, Client, ExpiredNonce, Start, request))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies timestamp overflow clamps retained expiry instead of rejecting a valid max timestamp.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestTimestampIsMaximumValue_ThenExpiryIsClamped()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.MaxValue);
        var registry = CreateRegistry(clock);
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, DateTimeOffset.MaxValue, request)).IsFalse();
        await Assert.That(registry.IsReplay(Tenant, Client, Nonce, DateTimeOffset.MaxValue, request)).IsTrue();
    }

    /// <summary>Verifies identical replays are still allowed when the registry is at its count limit.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRegistryIsAtCountCapacity_ThenIdenticalReplayIsAllowed()
    {
        var registry = CreateRegistry(maximumEntries: 1);
        var request = CreateRequest();

        await Assert.That(registry.IsReplay(Tenant, Client, FirstNonce, Start, request)).IsFalse();
        await Assert.That(registry.IsReplay(Tenant, Client, FirstNonce, Start, request)).IsTrue();

        await Assert
            .That(() => registry.IsReplay(Tenant, Client, SecondNonce, Start, request))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies retained metadata bytes are accounted and bounded.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRegistryWouldExceedRetainedBytes_ThenNewRequestIsRejected()
    {
        var registry = CreateRegistry(maximumRetainedBytes: TooSmallRetainedByteLimit);
        var request = CreateRequest();

        await Assert
            .That(() => registry.IsReplay(ShortTenant, ShortClient, ShortNonce, Start, request))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(registry.RetainedBytes).IsEqualTo(0);
    }

    /// <summary>Verifies retained metadata accounting excludes caller request bodies.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestBodyIsLarge_ThenOnlyFingerprintAndKeyMetadataAreRetained()
    {
        var registry = CreateRegistry(maximumRequestBytes: LargeRequestBytes);
        var request = new byte[LargeRequestBytes];
        request[0] = RequestFirstByte;

        await Assert.That(registry.IsReplay(ShortTenant, ShortClient, ShortNonce, Start, request)).IsFalse();
        await Assert.That(registry.RetainedBytes).IsEqualTo(ShortRequestRetainedBytes);
    }

    /// <summary>Verifies canonical request bodies are bounded before hashing.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestBodyIsEmptyOrTooLarge_ThenItIsRejected()
    {
        var registry = CreateRegistry(maximumRequestBytes: 1);
        var oversizedRequest = CreateOversizedRequest();

        await Assert
            .That(() => registry.IsReplay(Tenant, Client, EmptyNonce, Start, ReadOnlyMemory<byte>.Empty))
            .ThrowsExactly<ArgumentException>();
        await Assert
            .That(() => registry.IsReplay(Tenant, Client, LargeNonce, Start, oversizedRequest))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies authenticated identity and nonce parameters are bounded and strict UTF-8.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestKeyIsInvalid_ThenItIsRejected()
    {
        var registry = CreateRegistry(maximumRequestBytes: 1);
        var request = CreateRequest();
        var oversizedIdentity = new string('a', OversizedIdentityLength);
        var oversizedNonce = new string('n', OversizedNonceLength);
        var malformed = new string(MalformedSurrogate, SingleCharacter);
        var malformedLow = new string(MalformedLowSurrogate, SingleCharacter);

        await Assert.That(() => registry.IsReplay(BlankKey, Client, Nonce, Start, request)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => registry.IsReplay(oversizedIdentity, Client, Nonce, Start, request)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => registry.IsReplay(Tenant, oversizedIdentity, Nonce, Start, request)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => registry.IsReplay(Tenant, Client, oversizedNonce, Start, request)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => registry.IsReplay(malformed, Client, Nonce, Start, request)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => registry.IsReplay(Tenant, malformed, Nonce, Start, request)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => registry.IsReplay(Tenant, Client, malformed, Start, request)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => registry.IsReplay(malformedLow, Client, Nonce, Start, request)).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies valid non-BMP key text is accepted while malformed surrogate text is rejected.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenRequestKeyContainsValidSurrogatePair_ThenItIsAccepted()
    {
        var registry = CreateRegistry();
        var supplementary = char.ConvertFromUtf32(ValidSupplementaryCodePoint);

        await Assert.That(registry.IsReplay(supplementary, Client, Nonce, Start, CreateRequest())).IsFalse();
    }

    /// <summary>Verifies constructor parameters reject non-positive bounds.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenConstructorParametersAreInvalid_ThenItRejectsThem()
    {
        await Assert.That(static () => CreateRegistry(maximumEntries: 0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => CreateRegistry(maximumRetainedBytes: 0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => CreateRegistry(maximumRequestBytes: 0)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => CreateRegistry(freshnessWindow: TimeSpan.Zero)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => CreateRegistry(freshnessWindow: TimeSpan.MaxValue)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => CreateRegistry(nonceRetention: TimeSpan.Zero)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => CreateRegistry(nonceRetention: TimeSpan.MaxValue)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert
            .That(static () => new ReplayNonceRegistry(1, ConstructorRetainedBytes, 1, Window, Window, TimeProvider.System))
            .IsNotNull();
    }

    /// <summary>Verifies concurrent attempts admit one request and classify the rest as identical replays.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenConcurrentIdenticalRequestsArrive_ThenOnlyOneIsAdmitted()
    {
        var registry = CreateRegistry();
        using var ready = new ManualResetEventSlim();
        var tasks = new Task<bool>[ConcurrentCallCount];

        for (var index = 0; index < tasks.Length; index++)
        {
            tasks[index] = Task.Run(() =>
            {
                ready.Wait();
                return registry.IsReplay(Tenant, Client, Nonce, Start, CreateRequest());
            });
        }

        ready.Set();
        var replays = 0;
        var firstAdmissions = 0;
        foreach (var result in await Task.WhenAll(tasks))
        {
            if (result)
            {
                replays++;
            }
            else
            {
                firstAdmissions++;
            }
        }

        await Assert.That(firstAdmissions).IsEqualTo(1);
        await Assert.That(replays).IsEqualTo(ConcurrentCallCount - 1);
        await Assert.That(registry.Count).IsEqualTo(1);
    }

    /// <summary>Verifies the clock callback runs outside the registry gate.</summary>
    /// <returns>The asynchronous assertion task.</returns>
    [Test]
    public async Task WhenClockCallbackReentersRegistry_ThenItDoesNotRunInsideGate()
    {
        var clock = new ManualTimeProvider(Start);
        var registry = CreateRegistry(clock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        clock.BeforeRead = () =>
        {
            entered.SetResult();
            if (release.Wait(GuardTimeout))
            {
                return;
            }

            throw new TimeoutException("The clock callback did not finish.");
        };

        var request = Task.Factory.StartNew(
            static state => ((ReplayNonceRegistry)state!).IsReplay(Tenant, Client, Nonce, Start, CreateRequest()),
            registry,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
        await entered.Task.WaitAsync(GuardTimeout);
        try
        {
            var count = await Task.Run(() => registry.Count).WaitAsync(GuardTimeout);
            await Assert.That(count).IsEqualTo(0);
        }
        finally
        {
            release.Set();
        }

        await Assert.That(await request.WaitAsync(GuardTimeout)).IsFalse();
        await Assert.That(clock.ReadCount).IsEqualTo(1);
    }

    /// <summary>Creates a registry with test defaults.</summary>
    /// <param name="timeProvider">The optional manual time provider.</param>
    /// <param name="maximumEntries">The maximum retained entry count.</param>
    /// <param name="maximumRetainedBytes">The maximum retained metadata bytes.</param>
    /// <param name="maximumRequestBytes">The maximum canonical request body bytes.</param>
    /// <param name="freshnessWindow">The accepted request freshness window.</param>
    /// <param name="nonceRetention">The minimum retained nonce interval.</param>
    /// <returns>A configured replay nonce registry.</returns>
    private static ReplayNonceRegistry CreateRegistry(
        ManualTimeProvider? timeProvider = null,
        int maximumEntries = DefaultMaximumEntries,
        long maximumRetainedBytes = DefaultMaximumRetainedBytes,
        int maximumRequestBytes = DefaultMaximumRequestBytes,
        TimeSpan? freshnessWindow = null,
        TimeSpan? nonceRetention = null) =>
        new(maximumEntries, maximumRetainedBytes, maximumRequestBytes, freshnessWindow ?? Window, nonceRetention ?? Window, timeProvider ?? new(Start));

    /// <summary>Creates a fresh default canonical request buffer.</summary>
    /// <returns>A mutable request byte array.</returns>
    private static byte[] CreateRequest() => [RequestFirstByte, RequestSecondByte, RequestThirdByte];

    /// <summary>Creates a canonical request buffer that is too large for a one-byte limit.</summary>
    /// <returns>An oversized request byte array.</returns>
    private static byte[] CreateOversizedRequest() => [OversizedRequestFirstByte, OversizedRequestSecondByte];

    /// <summary>Manual clock used by replay registry tests.</summary>
    /// <param name="utcNow">The initial UTC timestamp.</param>
    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC timestamp returned by the provider.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <summary>The number of UTC reads observed by the provider.</summary>
        private int _readCount;

        /// <summary>Gets or sets a callback invoked before the current timestamp is returned.</summary>
        internal Action? BeforeRead { get; set; }

        /// <summary>Gets the number of UTC reads observed by the provider.</summary>
        internal int ReadCount => Volatile.Read(ref _readCount);

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow()
        {
            _ = Interlocked.Increment(ref _readCount);
            BeforeRead?.Invoke();
            return _utcNow;
        }

        /// <summary>Sets the current UTC timestamp returned by the provider.</summary>
        /// <param name="utcNow">The replacement UTC timestamp.</param>
        internal void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }
}
