// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Configures the occasionally-connected collaboration server example.</summary>
[System.Diagnostics.DebuggerDisplay("{ListenUri,nq} {DatabasePath,nq}")]
public sealed record CollaborationServerOptions
{
    /// <summary>The number of bytes in one kibibyte.</summary>
    private const int BytesPerKibibyte = 1024;

    /// <summary>The number of characters in a command-line option prefix.</summary>
    private const int ArgumentNameStartIndex = 2;

    /// <summary>The command-line switch used to configure the listening URL.</summary>
    private const string UrlArgumentName = "url";

    /// <summary>The command-line switch used to configure the SQLite journal path.</summary>
    private const string DatabaseArgumentName = "database";

    /// <summary>The command-line switch used to configure local development credentials.</summary>
    private const string CredentialsArgumentName = "credentials";

    /// <summary>The command-line switch used to configure the optional ASP.NET path base.</summary>
    private const string PathBaseArgumentName = "path-base";

    /// <summary>The default maximum operation count accepted in one push.</summary>
    private const int DefaultMaximumBatchOperations = 32;

    /// <summary>The default maximum concurrent non-acknowledgement request count.</summary>
    private const int DefaultMaximumConcurrentRequests = 8;

    /// <summary>The default reserved acknowledgement request capacity.</summary>
    private const int DefaultMaximumConcurrentAcknowledgements = 2;

    /// <summary>The default maximum active subscription count.</summary>
    private const int DefaultMaximumConcurrentSubscriptions = 8;

    /// <summary>The default maximum completed operation groups in one receive page.</summary>
    private const int DefaultMaximumReceiveGroups = 16;

    /// <summary>The default maximum event count in one receive page.</summary>
    private const int DefaultMaximumReceiveEvents = 64;

    /// <summary>The default maximum request size in kibibytes.</summary>
    private const int DefaultMaximumRequestKibibytes = 64;

    /// <summary>The default maximum payload size in kibibytes.</summary>
    private const int DefaultMaximumPayloadKibibytes = 16;

    /// <summary>The default server idempotency retention in minutes.</summary>
    private const int DefaultRetentionMinutes = 120;

    /// <summary>The default required client inbox retention in minutes.</summary>
    private const int DefaultInboxRetentionMinutes = 30;

    /// <summary>The default long-poll timeout in seconds.</summary>
    private const int DefaultLongPollSeconds = 30;

    /// <summary>The default empty-poll delay in milliseconds.</summary>
    private const int DefaultEmptyPollMilliseconds = 250;

    /// <summary>Gets the environment variable used to configure the listening URL.</summary>
    public static string UrlVariable => "OC_SERVER_URL";

    /// <summary>Gets the environment variable used to configure the server SQLite journal.</summary>
    public static string DatabaseVariable => "OC_SERVER_DATABASE";

    /// <summary>Gets the environment variable used to configure development credentials.</summary>
    public static string CredentialsVariable => "OC_DEMO_CREDENTIALS";

    /// <summary>Gets the environment variable used to configure the optional ASP.NET path base.</summary>
    public static string PathBaseVariable => "OC_SERVER_PATH_BASE";

    /// <summary>Gets the default loopback URL used for local development.</summary>
    public static string DefaultUrl => "http://127.0.0.1:5088";

    /// <summary>Gets the URL Kestrel binds to. The default is loopback HTTP.</summary>
    public Uri ListenUri { get; init; } = new(DefaultUrl);

    /// <summary>Gets the SQLite journal path. The server creates the parent directory but never deletes the database.</summary>
    public string DatabasePath { get; init; } = Path.Combine(AppContext.BaseDirectory, "occasionally-connected-server.db");

    /// <summary>Gets the optional relative ASP.NET path base where the protocol routes are mounted.</summary>
    public string PathBase { get; init; } = string.Empty;

    /// <summary>Gets the configured local development credentials.</summary>
    public IReadOnlyList<DevelopmentCredential> Credentials
    {
        get;
        init => field = CopyCredentials(value);
    } = [];

    /// <summary>Gets the maximum operation count accepted in one push.</summary>
    public int MaximumBatchOperations { get; init; } = DefaultMaximumBatchOperations;

    /// <summary>Gets the maximum request and response body bytes accepted by the portable HTTP protocol.</summary>
    public int MaximumRequestBytes { get; init; } = DefaultMaximumRequestKibibytes * BytesPerKibibyte;

    /// <summary>Gets the maximum payload bytes accepted by the portable HTTP protocol.</summary>
    public int MaximumPayloadBytes { get; init; } = DefaultMaximumPayloadKibibytes * BytesPerKibibyte;

    /// <summary>Gets the maximum concurrent non-acknowledgement HTTP requests.</summary>
    public int MaximumConcurrentRequests { get; init; } = DefaultMaximumConcurrentRequests;

    /// <summary>Gets the reserved concurrent acknowledgement request capacity.</summary>
    public int MaximumConcurrentAcknowledgements { get; init; } = DefaultMaximumConcurrentAcknowledgements;

    /// <summary>Gets the maximum active subscription polls.</summary>
    public int MaximumConcurrentSubscriptions { get; init; } = DefaultMaximumConcurrentSubscriptions;

    /// <summary>Gets the maximum completed operation groups returned in one receive page.</summary>
    public int MaximumReceiveGroups { get; init; } = DefaultMaximumReceiveGroups;

    /// <summary>Gets the maximum events returned in one receive page.</summary>
    public int MaximumReceiveEvents { get; init; } = DefaultMaximumReceiveEvents;

    /// <summary>Gets the finite server idempotency retention window.</summary>
    public TimeSpan ServerIdempotencyRetention { get; init; } = TimeSpan.FromMinutes(DefaultRetentionMinutes);

    /// <summary>Gets the finite client inbox retention required by this demo server.</summary>
    public TimeSpan ClientInboxRetentionRequired { get; init; } = TimeSpan.FromMinutes(DefaultInboxRetentionMinutes);

    /// <summary>Gets the finite long-poll timeout used by the HTTP endpoint.</summary>
    public TimeSpan LongPollTimeout { get; init; } = TimeSpan.FromSeconds(DefaultLongPollSeconds);

    /// <summary>Gets the bounded empty-poll delay used by the server hub.</summary>
    public TimeSpan EmptyPollDelay { get; init; } = TimeSpan.FromMilliseconds(DefaultEmptyPollMilliseconds);

    /// <summary>Creates options from process environment variables and command-line overrides.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <returns>The configured options.</returns>
    public static CollaborationServerOptions FromEnvironment(string[] args)
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is string key)
            {
                values[key] = entry.Value as string;
            }
        }

        return FromValues(values, args);
    }

    /// <summary>Creates options from name/value pairs and command-line overrides.</summary>
    /// <param name="values">The configuration values.</param>
    /// <param name="args">The command-line arguments.</param>
    /// <returns>The configured options.</returns>
    public static CollaborationServerOptions FromValues(IReadOnlyDictionary<string, string?> values, string[] args)
    {
        ArgumentNullException.ThrowIfNull(values);
        ArgumentNullException.ThrowIfNull(args);

        var combined = new Dictionary<string, string?>(values, StringComparer.OrdinalIgnoreCase);
        ApplyArguments(combined, args);

        var url = GetValue(combined, UrlVariable, UrlArgumentName) ?? DefaultUrl;
        var database = GetValue(combined, DatabaseVariable, DatabaseArgumentName)
            ?? Path.Combine(AppContext.BaseDirectory, "occasionally-connected-server.db");
        var credentialsText = GetValue(combined, CredentialsVariable, CredentialsArgumentName) ?? string.Empty;
        var pathBase = GetValue(combined, PathBaseVariable, PathBaseArgumentName) ?? string.Empty;

        var options = new CollaborationServerOptions
        {
            ListenUri = CreateLoopbackUri(url),
            DatabasePath = RequireText(database, DatabaseVariable),
            PathBase = NormalizePathBase(pathBase),
            Credentials = DevelopmentCredential.ParseMany(credentialsText),
            MaximumBatchOperations = GetPositiveInt(combined, "OC_SERVER_MAX_BATCH_OPERATIONS", DefaultMaximumBatchOperations),
            MaximumRequestBytes = GetPositiveKibibytesAsBytes(combined, "OC_SERVER_MAX_REQUEST_KIB", DefaultMaximumRequestKibibytes),
            MaximumPayloadBytes = GetPositiveKibibytesAsBytes(combined, "OC_SERVER_MAX_PAYLOAD_KIB", DefaultMaximumPayloadKibibytes),
            MaximumConcurrentRequests = GetPositiveInt(combined, "OC_SERVER_MAX_REQUESTS", DefaultMaximumConcurrentRequests),
            MaximumConcurrentAcknowledgements = GetPositiveInt(combined, "OC_SERVER_MAX_ACKS", DefaultMaximumConcurrentAcknowledgements),
            MaximumConcurrentSubscriptions = GetPositiveInt(combined, "OC_SERVER_MAX_SUBSCRIPTIONS", DefaultMaximumConcurrentSubscriptions),
            MaximumReceiveGroups = GetPositiveInt(combined, "OC_SERVER_MAX_RECEIVE_GROUPS", DefaultMaximumReceiveGroups),
            MaximumReceiveEvents = GetPositiveInt(combined, "OC_SERVER_MAX_RECEIVE_EVENTS", DefaultMaximumReceiveEvents),
            ServerIdempotencyRetention = GetPositiveMinutes(combined, "OC_SERVER_RETENTION_MINUTES", DefaultRetentionMinutes),
            ClientInboxRetentionRequired = GetPositiveMinutes(combined, "OC_SERVER_CLIENT_INBOX_MINUTES", DefaultInboxRetentionMinutes),
            LongPollTimeout = GetPositiveSeconds(combined, "OC_SERVER_LONG_POLL_SECONDS", DefaultLongPollSeconds),
            EmptyPollDelay = GetPositiveMilliseconds(combined, "OC_SERVER_EMPTY_POLL_MILLISECONDS", DefaultEmptyPollMilliseconds),
        };

        options.Validate();
        return options;
    }

    /// <summary>Validates the option set.</summary>
    /// <exception cref="ArgumentException">The URL, database path, credentials or limits are invalid.</exception>
    public void Validate()
    {
        ArgumentNullException.ThrowIfNull(ListenUri);
        if (!ListenUri.IsAbsoluteUri || !ListenUri.IsLoopback || !string.Equals(ListenUri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("The example server must bind to loopback HTTP unless the host code is changed deliberately.", nameof(ListenUri));
        }

        _ = RequireText(DatabasePath, nameof(DatabasePath));
        _ = NormalizePathBase(PathBase);
        if (Credentials.Count == 0)
        {
            throw new ArgumentException("Configure at least one development credential with OC_DEMO_CREDENTIALS.", nameof(Credentials));
        }

        DevelopmentCredential.ValidateAll(Credentials);
        ValidatePositive(MaximumBatchOperations, nameof(MaximumBatchOperations));
        ValidatePositive(MaximumRequestBytes, nameof(MaximumRequestBytes));
        ValidatePositive(MaximumPayloadBytes, nameof(MaximumPayloadBytes));
        ValidatePayloadLimit(MaximumPayloadBytes, MaximumRequestBytes);
        ValidatePositive(MaximumConcurrentRequests, nameof(MaximumConcurrentRequests));
        ValidatePositive(MaximumConcurrentAcknowledgements, nameof(MaximumConcurrentAcknowledgements));
        ValidatePositive(MaximumConcurrentSubscriptions, nameof(MaximumConcurrentSubscriptions));
        ValidatePositive(MaximumReceiveGroups, nameof(MaximumReceiveGroups));
        ValidatePositive(MaximumReceiveEvents, nameof(MaximumReceiveEvents));
        ValidatePositive(ServerIdempotencyRetention, nameof(ServerIdempotencyRetention));
        ValidatePositive(ClientInboxRetentionRequired, nameof(ClientInboxRetentionRequired));
        ValidatePositive(LongPollTimeout, nameof(LongPollTimeout));
        ValidatePositive(EmptyPollDelay, nameof(EmptyPollDelay));
    }

    /// <summary>Applies command-line overrides to the configuration values.</summary>
    /// <param name="values">The mutable values dictionary.</param>
    /// <param name="args">The command-line arguments.</param>
    /// <exception cref="ArgumentException">A command-line argument is unknown or missing its value.</exception>
    private static void ApplyArguments(Dictionary<string, string?> values, string[] args)
    {
        for (var index = 0; index < args.Length; index++)
        {
            var argument = args[index];
            if (!argument.StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException("Command-line arguments must use --name value or --name=value form.", nameof(args));
            }

            var separator = argument.IndexOf('=', StringComparison.Ordinal);
            if (separator > ArgumentNameStartIndex)
            {
                var name = RequireKnownArgument(argument[ArgumentNameStartIndex..separator]);
                var value = argument[(separator + 1)..];
                ArgumentException.ThrowIfNullOrWhiteSpace(value, nameof(args));

                values[name] = value;
                continue;
            }

            var bareName = RequireKnownArgument(argument[ArgumentNameStartIndex..]);
            if (index + 1 >= args.Length || args[index + 1].StartsWith("--", StringComparison.Ordinal))
            {
                throw new ArgumentException("Command-line arguments must include a value.", nameof(args));
            }

            index++;
            ArgumentException.ThrowIfNullOrWhiteSpace(args[index], nameof(args));

            values[bareName] = args[index];
        }
    }

    /// <summary>Validates that a command-line argument name is supported.</summary>
    /// <param name="name">The command-line argument name without the prefix.</param>
    /// <returns>The supported argument name.</returns>
    /// <exception cref="ArgumentException"><paramref name="name"/> is not supported.</exception>
    private static string RequireKnownArgument(string name) =>
        name switch
        {
            UrlArgumentName => UrlArgumentName,
            DatabaseArgumentName => DatabaseArgumentName,
            CredentialsArgumentName => CredentialsArgumentName,
            PathBaseArgumentName => PathBaseArgumentName,
            _ => throw new ArgumentException($"Unknown command-line argument '--{name}'.", nameof(name)),
        };

    /// <summary>Reads a command-line value before falling back to its environment variable.</summary>
    /// <param name="values">The combined values.</param>
    /// <param name="environmentName">The environment variable name.</param>
    /// <param name="argumentName">The command-line argument name.</param>
    /// <returns>The configured value, or <see langword="null"/> when absent.</returns>
    private static string? GetValue(Dictionary<string, string?> values, string environmentName, string argumentName)
    {
        if (values.TryGetValue(argumentName, out var argumentValue) && !string.IsNullOrWhiteSpace(argumentValue))
        {
            return argumentValue;
        }

        return values.TryGetValue(environmentName, out var environmentValue) && !string.IsNullOrWhiteSpace(environmentValue)
            ? environmentValue
            : null;
    }

    /// <summary>Normalizes an optional path base to the relative form used by the portable endpoint.</summary>
    /// <param name="value">The configured path base.</param>
    /// <returns>The normalized path base, or an empty string when no mount is configured.</returns>
    /// <exception cref="ArgumentException"><paramref name="value"/> is absolute URI text or only separators.</exception>
    private static string NormalizePathBase(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var relative = value.Trim().Trim('/');
        if (relative.Length == 0)
        {
            return string.Empty;
        }

        return Uri.TryCreate(relative, UriKind.Absolute, out _)
            ? throw new ArgumentException("The example server path base must be a relative path.", nameof(value))
            : relative;
    }

    /// <summary>Creates the configured loopback URI.</summary>
    /// <param name="value">The configured URI text.</param>
    /// <returns>The configured URI.</returns>
    /// <exception cref="ArgumentException"><paramref name="value"/> is not an absolute URI.</exception>
    private static Uri CreateLoopbackUri(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri))
        {
            throw new ArgumentException("The configured server URL must be an absolute URI.", UrlVariable);
        }

        return uri;
    }

    /// <summary>Reads a positive integer configuration value.</summary>
    /// <param name="values">The combined values.</param>
    /// <param name="key">The configuration key.</param>
    /// <param name="fallback">The fallback value.</param>
    /// <returns>The positive integer value.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The configured value is not positive.</exception>
    private static int GetPositiveInt(Dictionary<string, string?> values, string key, int fallback)
    {
        if (!values.TryGetValue(key, out var value) || string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        if (int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
        {
            return parsed;
        }

        throw new ArgumentOutOfRangeException(key, value, "The configured value must be a positive integer.");
    }

    /// <summary>Reads a positive KiB value and converts it to bytes with checked arithmetic.</summary>
    /// <param name="values">The combined values.</param>
    /// <param name="key">The configuration key.</param>
    /// <param name="fallback">The fallback KiB value.</param>
    /// <returns>The positive byte count.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The configured value is not positive.</exception>
    /// <exception cref="OverflowException">The configured KiB value cannot be represented as a byte count.</exception>
    private static int GetPositiveKibibytesAsBytes(Dictionary<string, string?> values, string key, int fallback) =>
        checked(GetPositiveInt(values, key, fallback) * BytesPerKibibyte);

    /// <summary>Reads a positive minute value.</summary>
    /// <param name="values">The combined values.</param>
    /// <param name="key">The configuration key.</param>
    /// <param name="fallback">The fallback value.</param>
    /// <returns>The configured minute duration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TimeSpan GetPositiveMinutes(Dictionary<string, string?> values, string key, int fallback) =>
        TimeSpan.FromMinutes(GetPositiveInt(values, key, fallback));

    /// <summary>Reads a positive second value.</summary>
    /// <param name="values">The combined values.</param>
    /// <param name="key">The configuration key.</param>
    /// <param name="fallback">The fallback value.</param>
    /// <returns>The configured second duration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TimeSpan GetPositiveSeconds(Dictionary<string, string?> values, string key, int fallback) =>
        TimeSpan.FromSeconds(GetPositiveInt(values, key, fallback));

    /// <summary>Reads a positive millisecond value.</summary>
    /// <param name="values">The combined values.</param>
    /// <param name="key">The configuration key.</param>
    /// <param name="fallback">The fallback value.</param>
    /// <returns>The configured millisecond duration.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TimeSpan GetPositiveMilliseconds(Dictionary<string, string?> values, string key, int fallback) =>
        TimeSpan.FromMilliseconds(GetPositiveInt(values, key, fallback));

    /// <summary>Requires non-empty text.</summary>
    /// <param name="value">The candidate text.</param>
    /// <param name="name">The option name used in exceptions.</param>
    /// <returns>The validated non-empty text.</returns>
    private static string RequireText(string value, string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, name);
        return value;
    }

    /// <summary>Copies credentials into an immutable collection.</summary>
    /// <param name="source">The source credential list.</param>
    /// <returns>The copied immutable credentials.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or an entry is <see langword="null"/>.</exception>
    private static ReadOnlyCollection<DevelopmentCredential> CopyCredentials(IReadOnlyList<DevelopmentCredential> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        var copy = new DevelopmentCredential[source.Count];
        for (var index = 0; index < source.Count; index++)
        {
            copy[index] = source[index] ?? throw new ArgumentNullException(nameof(source), "Credentials cannot contain null entries.");
        }

        return Array.AsReadOnly(copy);
    }

    /// <summary>Validates that an integer setting is positive.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="name">The option name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive.</exception>
    private static void ValidatePositive(int value, string name) =>
        _ = value <= 0 ? throw new ArgumentOutOfRangeException(name, value, "The configured value must be positive.") : true;

    /// <summary>Validates that a duration setting is positive.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="name">The option name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive.</exception>
    private static void ValidatePositive(TimeSpan value, string name) =>
        _ = value <= TimeSpan.Zero ? throw new ArgumentOutOfRangeException(name, value, "The configured value must be positive.") : true;

    /// <summary>Validates that decoded payloads cannot exceed the encoded request envelope budget.</summary>
    /// <param name="maximumPayloadBytes">The maximum decoded payload byte count.</param>
    /// <param name="maximumRequestBytes">The maximum encoded request byte count.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximumPayloadBytes"/> exceeds <paramref name="maximumRequestBytes"/>.</exception>
    private static void ValidatePayloadLimit(int maximumPayloadBytes, int maximumRequestBytes) =>
        _ = maximumPayloadBytes > maximumRequestBytes
            ? throw new ArgumentOutOfRangeException(nameof(MaximumPayloadBytes), maximumPayloadBytes, "The payload byte limit cannot exceed the request byte limit.")
            : true;
}
