// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="CollaborationServerOptions"/>.</summary>
public sealed class CollaborationServerOptionsTests
{
    /// <summary>The test token used when options require a credential.</summary>
    private const string TokenA = "token-a";

    /// <summary>The second test token used when credentials must be distinct.</summary>
    private const string TokenB = "token-b";

    /// <summary>The test tenant identifier used when options require a credential.</summary>
    private const string TenantA = "tenant-a";

    /// <summary>The first test client identifier used when options require a credential.</summary>
    private const string ClientA = "client-a";

    /// <summary>The second test client identifier used when duplicate token rejection is exercised.</summary>
    private const string ClientB = "client-b";

    /// <summary>The first serialized credential used by parser tests.</summary>
    private const string CredentialA = $"{TokenA}:{TenantA}:{ClientA}";

    /// <summary>The second serialized credential used by parser tests.</summary>
    private const string CredentialB = $"{TokenB}:{TenantA}:{ClientA}";

    /// <summary>The KiB value that wraps to a small positive byte count when multiplied unchecked.</summary>
    private const string PositiveWrappingKibibytes = "4194305";

    /// <summary>The command-line URL switch used by parser tests.</summary>
    private const string UrlArgument = "--url";

    /// <summary>The command-line database switch used by parser tests.</summary>
    private const string DatabaseArgument = "--database";

    /// <summary>The command-line credentials switch used by parser tests.</summary>
    private const string CredentialsArgument = "--credentials";

    /// <summary>The command-line path-base switch used by parser tests.</summary>
    private const string PathBaseArgument = "--path-base";

    /// <summary>The environment batch operation limit used by bounded parser tests.</summary>
    private const int EnvironmentBatchOperations = 3;

    /// <summary>The environment request-size limit in KiB used by bounded parser tests.</summary>
    private const int EnvironmentRequestKibibytes = 8;

    /// <summary>The environment payload-size limit in KiB used by bounded parser tests.</summary>
    private const int EnvironmentPayloadKibibytes = 4;

    /// <summary>The environment concurrent request limit used by bounded parser tests.</summary>
    private const int EnvironmentMaximumRequests = 5;

    /// <summary>The environment concurrent acknowledgement limit used by bounded parser tests.</summary>
    private const int EnvironmentMaximumAcknowledgements = 6;

    /// <summary>The environment concurrent subscription limit used by bounded parser tests.</summary>
    private const int EnvironmentMaximumSubscriptions = 7;

    /// <summary>The environment receive-group limit used by bounded parser tests.</summary>
    private const int EnvironmentMaximumReceiveGroups = 9;

    /// <summary>The environment receive-event limit used by bounded parser tests.</summary>
    private const int EnvironmentMaximumReceiveEvents = 10;

    /// <summary>The environment idempotency retention in minutes used by bounded parser tests.</summary>
    private const int EnvironmentRetentionMinutes = 11;

    /// <summary>The environment client inbox retention in minutes used by bounded parser tests.</summary>
    private const int EnvironmentClientInboxMinutes = 12;

    /// <summary>The environment long-poll timeout in seconds used by bounded parser tests.</summary>
    private const int EnvironmentLongPollSeconds = 13;

    /// <summary>The environment empty-poll delay in milliseconds used by bounded parser tests.</summary>
    private const int EnvironmentEmptyPollMilliseconds = 14;

    /// <summary>The batch operation limit used by parser override tests.</summary>
    private const int ExpectedBatchOperations = 17;

    /// <summary>The configured integer limit key used by parser rejection tests.</summary>
    private const string MaximumBatchOperationsVariable = "OC_SERVER_MAX_BATCH_OPERATIONS";

    /// <summary>The configured request-size limit key used by parser tests.</summary>
    private const string MaximumRequestKibibytesVariable = "OC_SERVER_MAX_REQUEST_KIB";

    /// <summary>The configured payload-size limit key used by parser tests.</summary>
    private const string MaximumPayloadKibibytesVariable = "OC_SERVER_MAX_PAYLOAD_KIB";

    /// <summary>The configured concurrent request limit key used by parser tests.</summary>
    private const string MaximumRequestsVariable = "OC_SERVER_MAX_REQUESTS";

    /// <summary>The configured concurrent acknowledgement limit key used by parser tests.</summary>
    private const string MaximumAcknowledgementsVariable = "OC_SERVER_MAX_ACKS";

    /// <summary>The configured concurrent subscription limit key used by parser tests.</summary>
    private const string MaximumSubscriptionsVariable = "OC_SERVER_MAX_SUBSCRIPTIONS";

    /// <summary>The configured receive-group limit key used by parser tests.</summary>
    private const string MaximumReceiveGroupsVariable = "OC_SERVER_MAX_RECEIVE_GROUPS";

    /// <summary>The configured receive-event limit key used by parser tests.</summary>
    private const string MaximumReceiveEventsVariable = "OC_SERVER_MAX_RECEIVE_EVENTS";

    /// <summary>The configured idempotency retention key used by parser tests.</summary>
    private const string RetentionMinutesVariable = "OC_SERVER_RETENTION_MINUTES";

    /// <summary>The configured client inbox retention key used by parser tests.</summary>
    private const string ClientInboxMinutesVariable = "OC_SERVER_CLIENT_INBOX_MINUTES";

    /// <summary>The configured long-poll timeout key used by parser tests.</summary>
    private const string LongPollSecondsVariable = "OC_SERVER_LONG_POLL_SECONDS";

    /// <summary>The configured empty-poll delay key used by parser tests.</summary>
    private const string EmptyPollMillisecondsVariable = "OC_SERVER_EMPTY_POLL_MILLISECONDS";

    /// <summary>The expected normalized mounted path base used by parser tests.</summary>
    private const string ExpectedPathBase = "server/oc";

    /// <summary>The number of bytes in one kibibyte.</summary>
    private const int BytesPerKibibyte = 1024;

    /// <summary>The request byte limit used by payload coherence tests.</summary>
    private const int PayloadCoherenceRequestBytes = BytesPerKibibyte;

    /// <summary>The payload byte limit that exceeds the request budget in coherence tests.</summary>
    private const int PayloadAboveRequestBytes = 2 * BytesPerKibibyte;

    /// <summary>Verifies local credentials are mandatory and are not supplied by source defaults.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRequiresDevelopmentCredentials()
    {
        var values = CreateValuesWithDatabaseOnly();

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [])).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies command-line arguments create finite loopback server options.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesAppliesCommandLineOverridesAndFiniteBudgets()
    {
        var database = CreateDatabasePath();
        var options = CollaborationServerOptions.FromValues(
            CreateEmptyValues(),
            [UrlArgument, "http://127.0.0.1:0", DatabaseArgument, database, CredentialsArgument, CredentialA, PathBaseArgument, $"/{ExpectedPathBase}/"]);

        await Assert.That(options.ListenUri.IsLoopback).IsTrue();
        await Assert.That(options.DatabasePath).IsEqualTo(database);
        await Assert.That(options.PathBase).IsEqualTo(ExpectedPathBase);
        await Assert.That(options.Credentials).Count().IsEqualTo(1);
        await Assert.That(options.MaximumBatchOperations).IsGreaterThan(0);
        await Assert.That(options.MaximumRequestBytes).IsGreaterThan(options.MaximumPayloadBytes);
        await Assert.That(options.ServerIdempotencyRetention).IsGreaterThan(TimeSpan.Zero);
        await Assert.That(options.ClientInboxRetentionRequired).IsGreaterThan(TimeSpan.Zero);
    }

    /// <summary>Verifies command-line equals-form arguments override environment variables.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesAcceptsEqualsFormCommandLineOverrides()
    {
        var environmentDatabase = CreateDatabasePath();
        var commandLineDatabase = CreateDatabasePath();
        var values = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [CollaborationServerOptions.UrlVariable] = "http://127.0.0.1:1",
            [CollaborationServerOptions.DatabaseVariable] = environmentDatabase,
            [CollaborationServerOptions.CredentialsVariable] = CredentialB,
            [MaximumBatchOperationsVariable] = ExpectedBatchOperations.ToString(CultureInfo.InvariantCulture),
        };

        var options = CollaborationServerOptions.FromValues(
            values,
            [$"{UrlArgument}={CollaborationServerOptions.DefaultUrl}", $"{DatabaseArgument}={commandLineDatabase}", $"{CredentialsArgument}={CredentialA}"]);

        await Assert.That(options.ListenUri).IsEqualTo(new(CollaborationServerOptions.DefaultUrl));
        await Assert.That(options.DatabasePath).IsEqualTo(commandLineDatabase);
        await Assert.That(options.Credentials[0].Token).IsEqualTo(TokenA);
        await Assert.That(options.MaximumBatchOperations).IsEqualTo(ExpectedBatchOperations);
    }

    /// <summary>Verifies all bounded environment values flow into runtime options.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesAppliesBoundedEnvironmentLimits()
    {
        var values = CreateRequiredValues();
        values[MaximumBatchOperationsVariable] = EnvironmentBatchOperations.ToString(CultureInfo.InvariantCulture);
        values[MaximumRequestKibibytesVariable] = EnvironmentRequestKibibytes.ToString(CultureInfo.InvariantCulture);
        values[MaximumPayloadKibibytesVariable] = EnvironmentPayloadKibibytes.ToString(CultureInfo.InvariantCulture);
        values[MaximumRequestsVariable] = EnvironmentMaximumRequests.ToString(CultureInfo.InvariantCulture);
        values[MaximumAcknowledgementsVariable] = EnvironmentMaximumAcknowledgements.ToString(CultureInfo.InvariantCulture);
        values[MaximumSubscriptionsVariable] = EnvironmentMaximumSubscriptions.ToString(CultureInfo.InvariantCulture);
        values[MaximumReceiveGroupsVariable] = EnvironmentMaximumReceiveGroups.ToString(CultureInfo.InvariantCulture);
        values[MaximumReceiveEventsVariable] = EnvironmentMaximumReceiveEvents.ToString(CultureInfo.InvariantCulture);
        values[RetentionMinutesVariable] = EnvironmentRetentionMinutes.ToString(CultureInfo.InvariantCulture);
        values[ClientInboxMinutesVariable] = EnvironmentClientInboxMinutes.ToString(CultureInfo.InvariantCulture);
        values[LongPollSecondsVariable] = EnvironmentLongPollSeconds.ToString(CultureInfo.InvariantCulture);
        values[EmptyPollMillisecondsVariable] = EnvironmentEmptyPollMilliseconds.ToString(CultureInfo.InvariantCulture);
        values[CollaborationServerOptions.PathBaseVariable] = $"/{ExpectedPathBase}/";

        var options = CollaborationServerOptions.FromValues(values, []);

        await Assert.That(options.MaximumBatchOperations).IsEqualTo(EnvironmentBatchOperations);
        await Assert.That(options.MaximumRequestBytes).IsEqualTo(EnvironmentRequestKibibytes * BytesPerKibibyte);
        await Assert.That(options.MaximumPayloadBytes).IsEqualTo(EnvironmentPayloadKibibytes * BytesPerKibibyte);
        await Assert.That(options.MaximumConcurrentRequests).IsEqualTo(EnvironmentMaximumRequests);
        await Assert.That(options.MaximumConcurrentAcknowledgements).IsEqualTo(EnvironmentMaximumAcknowledgements);
        await Assert.That(options.MaximumConcurrentSubscriptions).IsEqualTo(EnvironmentMaximumSubscriptions);
        await Assert.That(options.MaximumReceiveGroups).IsEqualTo(EnvironmentMaximumReceiveGroups);
        await Assert.That(options.MaximumReceiveEvents).IsEqualTo(EnvironmentMaximumReceiveEvents);
        await Assert.That(options.ServerIdempotencyRetention).IsEqualTo(TimeSpan.FromMinutes(EnvironmentRetentionMinutes));
        await Assert.That(options.ClientInboxRetentionRequired).IsEqualTo(TimeSpan.FromMinutes(EnvironmentClientInboxMinutes));
        await Assert.That(options.LongPollTimeout).IsEqualTo(TimeSpan.FromSeconds(EnvironmentLongPollSeconds));
        await Assert.That(options.EmptyPollDelay).IsEqualTo(TimeSpan.FromMilliseconds(EnvironmentEmptyPollMilliseconds));
        await Assert.That(options.PathBase).IsEqualTo(ExpectedPathBase);
    }

    /// <summary>Verifies the database path falls back to the application base directory when only credentials are configured.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesUsesDatabaseFallbackWhenOnlyCredentialsAreConfigured()
    {
        var values = new Dictionary<string, string?>(StringComparer.Ordinal) { [CollaborationServerOptions.CredentialsVariable] = CredentialA };

        var options = CollaborationServerOptions.FromValues(values, []);

        await Assert.That(options.DatabasePath).IsEqualTo(
            System.IO.Path.Combine(AppContext.BaseDirectory, "occasionally-connected-server.db"));
        await Assert.That(options.Credentials).Count().IsEqualTo(1);
    }

    /// <summary>Verifies process environment variables can configure runnable options.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromEnvironmentReadsProcessVariables()
    {
        var previousUrl = Environment.GetEnvironmentVariable(CollaborationServerOptions.UrlVariable);
        var previousDatabase = Environment.GetEnvironmentVariable(CollaborationServerOptions.DatabaseVariable);
        var previousCredentials = Environment.GetEnvironmentVariable(CollaborationServerOptions.CredentialsVariable);
        var previousPathBase = Environment.GetEnvironmentVariable(CollaborationServerOptions.PathBaseVariable);
        var database = CreateDatabasePath();
        try
        {
            Environment.SetEnvironmentVariable(CollaborationServerOptions.UrlVariable, CollaborationServerOptions.DefaultUrl);
            Environment.SetEnvironmentVariable(CollaborationServerOptions.DatabaseVariable, database);
            Environment.SetEnvironmentVariable(CollaborationServerOptions.CredentialsVariable, CredentialA);
            Environment.SetEnvironmentVariable(CollaborationServerOptions.PathBaseVariable, ExpectedPathBase);

            var options = CollaborationServerOptions.FromEnvironment([]);

            await Assert.That(options.ListenUri).IsEqualTo(new(CollaborationServerOptions.DefaultUrl));
            await Assert.That(options.DatabasePath).IsEqualTo(database);
            await Assert.That(options.PathBase).IsEqualTo(ExpectedPathBase);
            await Assert.That(options.Credentials[0].ClientId).IsEqualTo(ClientA);
        }
        finally
        {
            Environment.SetEnvironmentVariable(CollaborationServerOptions.UrlVariable, previousUrl);
            Environment.SetEnvironmentVariable(CollaborationServerOptions.DatabaseVariable, previousDatabase);
            Environment.SetEnvironmentVariable(CollaborationServerOptions.CredentialsVariable, previousCredentials);
            Environment.SetEnvironmentVariable(CollaborationServerOptions.PathBaseVariable, previousPathBase);
        }
    }

    /// <summary>Verifies non-loopback hosts are rejected by the development example.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsNonLoopbackListenUri()
    {
        var options = CreateValidOptions() with { ListenUri = new("http://example.com") };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies manually constructed credentials are validated before runtime resources are opened.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsBlankManuallyConstructedCredentials()
    {
        var options = CreateValidOptions() with { Credentials = [new(" ", TenantA, ClientA)] };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies manually assigned credentials are copied only after every entry is present.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CredentialsInitRejectsNullCredentialEntries()
    {
        var credentials = CreateCredentialsWithNullEntry();

        await Assert.That(() => CreateValidOptions() with { Credentials = credentials }).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies duplicate tokens are rejected before the SQLite journal directory is created.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RuntimeCreateRejectsDuplicateCredentialsBeforeDatabaseSideEffects()
    {
        var directory = OwnedTempDirectory.CreatePath("rxui-oc-server-duplicate-");
        var options = new CollaborationServerOptions
        {
            ListenUri = new(CollaborationServerOptions.DefaultUrl),
            DatabasePath = System.IO.Path.Combine(directory, "journal.db"),
            Credentials = [new(TokenA, TenantA, ClientA), new(TokenA, TenantA, ClientB)],
        };

        try
        {
            _ = await Assert.ThrowsExactlyAsync<ArgumentException>(async () =>
                await CollaborationServerRuntime.CreateAsync(options, CancellationToken.None).ConfigureAwait(false));
            await Assert.That(Directory.Exists(directory)).IsFalse();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>Verifies unknown command-line switches are rejected instead of being silently ignored.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsUnknownCommandLineArgument()
    {
        var values = CreateRequiredValues();

        await Assert.That(() => CollaborationServerOptions.FromValues(values, ["--unknown", "value"])).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies command-line switches without values are rejected even when an environment fallback exists.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsIncompleteCommandLineArgument()
    {
        var values = CreateRequiredValues();

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [UrlArgument])).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies a command-line switch followed by another switch is still missing a value.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsCommandLineArgumentWhenNextTokenIsSwitch()
    {
        var values = CreateRequiredValues();

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [UrlArgument, DatabaseArgument, CreateDatabasePath()]))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies bare command-line switch values must contain concrete text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsWhitespaceCommandLineArgumentValue()
    {
        var values = CreateRequiredValues();

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [UrlArgument, " "])).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies KiB-to-byte conversion uses checked arithmetic for oversized input.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsOverflowingRequestKibibytes()
    {
        var values = CreateRequiredValues();
        values[MaximumRequestKibibytesVariable] = int.MaxValue.ToString(CultureInfo.InvariantCulture);

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [])).ThrowsExactly<OverflowException>();
    }

    /// <summary>Verifies request-size KiB values that wrap to a positive byte count are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsPositiveWrappedRequestKibibytes()
    {
        var values = CreateRequiredValues();
        values[MaximumRequestKibibytesVariable] = PositiveWrappingKibibytes;

        await Assert.That(AcceptsValues(values)).IsFalse();
    }

    /// <summary>Verifies payload-size KiB values that wrap to a positive byte count are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsPositiveWrappedPayloadKibibytes()
    {
        var values = CreateRequiredValues();
        values[MaximumPayloadKibibytesVariable] = PositiveWrappingKibibytes;

        await Assert.That(AcceptsValues(values)).IsFalse();
    }

    /// <summary>Verifies integer limits must parse as positive invariant integers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsInvalidIntegerLimit()
    {
        var values = CreateRequiredValues();
        values[MaximumBatchOperationsVariable] = "not-a-number";

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [])).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies command-line arguments without the required switch prefix are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsArgumentWithoutSwitchPrefix()
    {
        var values = CreateRequiredValues();

        await Assert.That(() => CollaborationServerOptions.FromValues(
            values,
            ["url", CollaborationServerOptions.DefaultUrl])).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies --name=value command-line arguments must include a concrete value.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsEqualsArgumentWithoutValue()
    {
        var values = CreateRequiredValues();

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [$"{UrlArgument}="])).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies configured URL text must be absolute URI text.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FromValuesRejectsInvalidUrlText()
    {
        var values = CreateRequiredValues();
        values[CollaborationServerOptions.UrlVariable] = "not a url";

        await Assert.That(() => CollaborationServerOptions.FromValues(values, [])).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies manually constructed options reject non-positive integer limits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsNonPositiveIntegerLimits()
    {
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumBatchOperations = 0 }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumRequestBytes = 0 }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumPayloadBytes = 0 }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumConcurrentRequests = 0 }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumConcurrentAcknowledgements = 0 }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumConcurrentSubscriptions = 0 }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumReceiveGroups = 0 }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { MaximumReceiveEvents = 0 }).ConfigureAwait(false);
    }

    /// <summary>Verifies decoded payload limits cannot exceed the encoded request envelope limit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsPayloadLimitAboveRequestLimit()
    {
        var options = CreateValidOptions() with { MaximumRequestBytes = PayloadCoherenceRequestBytes, MaximumPayloadBytes = PayloadAboveRequestBytes };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies the development example rejects HTTPS even on loopback unless host code changes deliberately.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsLoopbackHttpsListenUri()
    {
        var options = CreateValidOptions() with { ListenUri = new("https://127.0.0.1:5088") };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies mounted path bases must remain relative route prefixes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsAbsolutePathBase()
    {
        var options = CreateValidOptions() with { PathBase = "http://example.com/server" };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies manually constructed options reject non-positive time limits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateRejectsNonPositiveTimeLimits()
    {
        await AssertInvalidOptionsAsync(CreateValidOptions() with { ServerIdempotencyRetention = TimeSpan.Zero }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { ClientInboxRetentionRequired = TimeSpan.Zero }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { LongPollTimeout = TimeSpan.Zero }).ConfigureAwait(false);
        await AssertInvalidOptionsAsync(CreateValidOptions() with { EmptyPollDelay = TimeSpan.Zero }).ConfigureAwait(false);
    }

    /// <summary>Creates a unique database path for a test without creating the directory.</summary>
    /// <returns>The unique database path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateDatabasePath() =>
        OwnedTempDirectory.CreateDatabasePath("rxui-oc-server-options-");

    /// <summary>Creates a valid manually constructed option set.</summary>
    /// <returns>The valid option set.</returns>
    private static CollaborationServerOptions CreateValidOptions() =>
        new() { ListenUri = new(CollaborationServerOptions.DefaultUrl), DatabasePath = CreateDatabasePath(), Credentials = [new(TokenA, TenantA, ClientA)] };

    /// <summary>Creates a credential array containing a default null reference.</summary>
    /// <returns>The credential array.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DevelopmentCredential[] CreateCredentialsWithNullEntry() =>
        new DevelopmentCredential[1];

    /// <summary>Asserts the supplied options fail validation with an out-of-range exception.</summary>
    /// <param name="options">The options under test.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertInvalidOptionsAsync(CollaborationServerOptions options) =>
        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Creates an empty option value map.</summary>
    /// <returns>The empty option value map.</returns>
    private static Dictionary<string, string?> CreateEmptyValues() => [];

    /// <summary>Creates an option value map containing only the database path.</summary>
    /// <returns>The option value map.</returns>
    private static Dictionary<string, string?> CreateValuesWithDatabaseOnly()
    {
        var values = new Dictionary<string, string?>(StringComparer.Ordinal);
        values[CollaborationServerOptions.DatabaseVariable] = CreateDatabasePath();
        return values;
    }

    /// <summary>Creates the minimum environment map required to parse options.</summary>
    /// <returns>The option value map.</returns>
    private static Dictionary<string, string?> CreateRequiredValues()
    {
        var values = new Dictionary<string, string?>(StringComparer.Ordinal);
        values[CollaborationServerOptions.DatabaseVariable] = CreateDatabasePath();
        values[CollaborationServerOptions.CredentialsVariable] = CredentialB;
        return values;
    }

    /// <summary>Attempts to create server options from the supplied values.</summary>
    /// <param name="values">The option values.</param>
    /// <returns><see langword="true"/> when options are accepted.</returns>
    private static bool AcceptsValues(IReadOnlyDictionary<string, string?> values)
    {
        try
        {
            _ = CollaborationServerOptions.FromValues(values, []);
            return true;
        }
        catch (Exception exception) when (exception is ArgumentException or OverflowException)
        {
            return false;
        }
    }
}
