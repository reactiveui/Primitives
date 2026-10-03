// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Creates payload envelopes for the custom activity stream.</summary>
public static class ActivityPayloads
{
    /// <summary>The number of hash bytes used to create deterministic event identifiers.</summary>
    private const int EventIdByteCount = 16;

    /// <summary>The number of bytes returned by SHA-256.</summary>
    private const int Sha256ByteCount = 32;

    /// <summary>The activity schema version supported by this example.</summary>
    private const int SchemaVersion = 1;

    /// <summary>The maximum accepted activity JSON payload size.</summary>
    private const int MaximumPayloadBytes = 4096;

    /// <summary>The maximum accepted text length for activity fields.</summary>
    private const int MaximumTextLength = 256;

    /// <summary>The SHA-256 hash prefix used by payload envelopes.</summary>
    private const string Sha256Prefix = "sha256-";

    /// <summary>The compact GUID format used in canonical activity JSON.</summary>
    private const string GuidCompactFormat = "N";

    /// <summary>The round-trip date/time format used in canonical activity JSON.</summary>
    private const string RoundTripDateTimeFormat = "O";

    /// <summary>Gets the custom activity contract identifier.</summary>
    public static string ContractId => "example.collaboration.activity";

    /// <summary>Gets the custom activity content type.</summary>
    public static string ContentType => "application/vnd.reactiveui.oc.example.activity+json";

    /// <summary>Creates a UTF-8 activity payload envelope.</summary>
    /// <param name="json">The JSON payload.</param>
    /// <returns>The payload envelope.</returns>
    public static PayloadEnvelope Create(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);
        var bytes = Encoding.UTF8.GetBytes(json);
        var envelope = CreateEnvelope(bytes);
        _ = ReadInput(envelope);
        return envelope;
    }

    /// <summary>Creates a stable server event id for an accepted custom operation.</summary>
    /// <param name="clientId">The authenticated client identifier.</param>
    /// <param name="operation">The accepted operation.</param>
    /// <returns>The deterministic event identifier.</returns>
    public static Guid CreateDeterministicEventId(string clientId, SyncOperation operation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        ArgumentNullException.ThrowIfNull(operation);
        var text = $"{operation.StreamId.Value}:{clientId}:{operation.OperationId.Value:N}:activity";
        var bytes = Encoding.UTF8.GetBytes(text);
#if NET8_0_OR_GREATER
        Span<byte> hash = stackalloc byte[Sha256ByteCount];
        _ = SHA256.TryHashData(bytes, hash, out _);
#else
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(bytes);
#endif
        return new(hash[..EventIdByteCount]);
    }

    /// <summary>Creates the canonical initial activity stream state.</summary>
    /// <returns>The initial canonical payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static PayloadEnvelope CreateInitialState() =>
        CreateCanonicalEnvelope(new()
        {
            Status = "empty",
            AcceptedClientId = "server",
            AcceptedOperationId = Guid.Empty.ToString(GuidCompactFormat),
            AcceptedVersion = "activity-v0",
            ServerAcceptedUtc = DateTimeOffset.UnixEpoch.ToString(RoundTripDateTimeFormat, CultureInfo.InvariantCulture),
        });

    /// <summary>Creates canonical server state from an accepted activity operation.</summary>
    /// <param name="input">The validated input payload.</param>
    /// <param name="context">The server domain apply context.</param>
    /// <returns>The canonical payload envelope.</returns>
    internal static PayloadEnvelope CreateCanonical(ActivityPayloadInput input, ServerDomainApplyContext context)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);
        return CreateCanonical(input, context.Client, context.Conflict, context.Operation, context.Resolution.ServerVersion);
    }

    /// <summary>Creates canonical server state from accepted activity conflict inputs.</summary>
    /// <param name="input">The validated input payload.</param>
    /// <param name="client">The trusted client context supplied by the server.</param>
    /// <param name="conflict">The conflict context.</param>
    /// <param name="operation">The accepted operation.</param>
    /// <param name="serverVersion">The accepted server version.</param>
    /// <returns>The canonical payload envelope.</returns>
    internal static PayloadEnvelope CreateCanonical(
        ActivityPayloadInput input,
        ClientIdentity client,
        ConflictContext conflict,
        SyncOperation operation,
        string serverVersion)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(conflict);
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentException.ThrowIfNullOrWhiteSpace(serverVersion);
        var current = ReadCanonical(conflict.Current.State);
        var merged = Merge(input, current, client, conflict, operation, serverVersion);
        return CreateCanonicalEnvelope(merged);
    }

    /// <summary>Gets the canonical payload already produced by conflict resolution, or creates it for direct accepted operations.</summary>
    /// <param name="context">The server domain apply context.</param>
    /// <returns>The canonical payload envelope.</returns>
    /// <exception cref="InvalidOperationException">The resolved state metadata does not match the trusted context.</exception>
    internal static PayloadEnvelope GetResolvedCanonical(ServerDomainApplyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        for (var index = 0; index < context.Resolution.Conflicts.Count; index++)
        {
            var conflict = context.Resolution.Conflicts[index];
            if (conflict.OperationId != context.Operation.OperationId)
            {
                continue;
            }

            if (conflict.ResolvedPayload is not { } payload)
            {
                throw new InvalidOperationException("Activity conflict resolutions require a resolved canonical payload.");
            }

            var state = ReadCanonical(payload);
            ValidateResolvedCanonical(state, context);
            return payload;
        }

        var input = ReadInput(context.Operation.Payload);
        return CreateCanonical(input, context);
    }

    /// <summary>Reads and validates an activity input payload.</summary>
    /// <param name="envelope">The payload envelope.</param>
    /// <returns>The validated activity input.</returns>
    /// <exception cref="ArgumentException">The payload envelope does not match the activity schema.</exception>
    internal static ActivityPayloadInput ReadInput(PayloadEnvelope envelope)
    {
        if (!TryReadInput(envelope, out var input, out var reasonCode))
        {
            throw new ArgumentException($"Invalid activity payload: {reasonCode}.", nameof(envelope));
        }

        return input;
    }

    /// <summary>Attempts to read and validate an activity input payload.</summary>
    /// <param name="envelope">The payload envelope.</param>
    /// <param name="input">The validated input when successful.</param>
    /// <param name="reasonCode">The stable rejection reason when validation fails.</param>
    /// <returns>Whether the payload was valid.</returns>
    internal static bool TryReadInput(
        PayloadEnvelope envelope,
        out ActivityPayloadInput input,
        out string reasonCode)
    {
        input = new();
        reasonCode = string.Empty;
        if (!HasExpectedEnvelope(envelope, out reasonCode))
        {
            return false;
        }

        if (!ValidateHash(envelope, out reasonCode))
        {
            return false;
        }

        return TryReadInputPayload(envelope.Payload, out input, out reasonCode);
    }

    /// <summary>Validates resolved canonical metadata against the trusted context.</summary>
    /// <param name="state">The resolved canonical state.</param>
    /// <param name="context">The domain apply context.</param>
    /// <exception cref="InvalidOperationException">The resolved state metadata does not match the trusted context.</exception>
    private static void ValidateResolvedCanonical(ActivityCanonicalState state, ServerDomainApplyContext context)
    {
        var trustedAcceptedUtc = GetAcceptedUtc(context.Conflict, context.Operation);
        if (!DateTimeOffset.TryParse(
                state.ServerAcceptedUtc,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out var resolvedAcceptedUtc)
            || resolvedAcceptedUtc != trustedAcceptedUtc)
        {
            throw new InvalidOperationException("Activity resolved canonical metadata must match trusted server context.");
        }

        if (string.Equals(state.AcceptedClientId, context.Client.ClientId, StringComparison.Ordinal)
            && string.Equals(state.AcceptedOperationId, context.Operation.OperationId.Value.ToString(GuidCompactFormat), StringComparison.Ordinal)
            && string.Equals(state.AcceptedVersion, context.Resolution.ServerVersion, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("Activity resolved canonical metadata must match trusted server context.");
    }

    /// <summary>Creates the SHA-256 payload hash string.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <returns>The payload hash string.</returns>
    private static string CreateHash(byte[] bytes)
    {
#if NET8_0_OR_GREATER
        var hash = SHA256.HashData(bytes);
#else
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(bytes);
#endif
        return $"{Sha256Prefix}{Convert.ToBase64String(hash)}";
    }

    /// <summary>Creates a payload envelope for serialized activity bytes.</summary>
    /// <param name="bytes">The serialized payload bytes.</param>
    /// <returns>The activity payload envelope.</returns>
    private static PayloadEnvelope CreateEnvelope(byte[] bytes) =>
        new(ContractId, SchemaVersion, ContentType, bytes, CreateHash(bytes));

    /// <summary>Creates an activity envelope from canonical state.</summary>
    /// <param name="state">The canonical state.</param>
    /// <returns>The canonical envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static PayloadEnvelope CreateCanonicalEnvelope(ActivityCanonicalState state) =>
        CreateEnvelope(JsonSerializer.SerializeToUtf8Bytes(state, ActivityJsonContext.Default.ActivityCanonicalState));

    /// <summary>Validates the envelope metadata before payload parsing.</summary>
    /// <param name="envelope">The payload envelope.</param>
    /// <param name="reasonCode">The stable rejection reason when validation fails.</param>
    /// <returns>Whether the envelope metadata is valid.</returns>
    private static bool HasExpectedEnvelope(PayloadEnvelope envelope, out string reasonCode)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        if (!string.Equals(envelope.ContractId, ContractId, StringComparison.Ordinal))
        {
            reasonCode = "activity-contract-mismatch";
            return false;
        }

        if (envelope.SchemaVersion != SchemaVersion)
        {
            reasonCode = "activity-schema-version-mismatch";
            return false;
        }

        if (!string.Equals(envelope.ContentType, ContentType, StringComparison.Ordinal))
        {
            reasonCode = "activity-content-type-mismatch";
            return false;
        }

        if (envelope.PayloadLength is <= 0 or > MaximumPayloadBytes)
        {
            reasonCode = "activity-payload-size";
            return false;
        }

        reasonCode = string.Empty;
        return true;
    }

    /// <summary>Checks required activity text.</summary>
    /// <param name="value">The text value.</param>
    /// <returns>Whether the text is present and bounded.</returns>
    private static bool HasRequiredText(string? value) =>
        !string.IsNullOrWhiteSpace(value) && value.Length <= MaximumTextLength;

    /// <summary>Checks optional activity text.</summary>
    /// <param name="value">The text value.</param>
    /// <returns>Whether the text is absent or bounded.</returns>
    private static bool HasOptionalText(string? value) =>
        string.IsNullOrEmpty(value) || value.Length <= MaximumTextLength;

    /// <summary>Validates the payload hash against the serialized bytes.</summary>
    /// <param name="envelope">The payload envelope.</param>
    /// <param name="reasonCode">The stable rejection reason when validation fails.</param>
    /// <returns>Whether the hash matches the payload.</returns>
    private static bool ValidateHash(PayloadEnvelope envelope, out string reasonCode)
    {
        var bytes = envelope.Payload.ToArray();
        var expected = Encoding.UTF8.GetBytes(CreateHash(bytes));
        var actual = Encoding.UTF8.GetBytes(envelope.PayloadHash);
        if (CryptographicOperations.FixedTimeEquals(actual, expected))
        {
            reasonCode = string.Empty;
            return true;
        }

        reasonCode = "activity-payload-hash-mismatch";
        return false;
    }

    /// <summary>Reads the canonical state already stored by the server.</summary>
    /// <param name="envelope">The canonical payload envelope.</param>
    /// <returns>The validated canonical state.</returns>
    /// <exception cref="ArgumentException">The canonical payload does not match the activity schema.</exception>
    private static ActivityCanonicalState ReadCanonical(PayloadEnvelope envelope)
    {
        if (!HasExpectedEnvelope(envelope, out var reasonCode) || !ValidateHash(envelope, out reasonCode))
        {
            throw new ArgumentException($"Invalid activity canonical state: {reasonCode}.", nameof(envelope));
        }

        try
        {
            var state = JsonSerializer.Deserialize(envelope.Payload.Span, ActivityJsonContext.Default.ActivityCanonicalState);
            if (state is null || !IsCanonicalStateValid(state))
            {
                throw new ArgumentException("Invalid activity canonical state: activity-current-canonical-invalid.", nameof(envelope));
            }

            return state;
        }
        catch (JsonException exception)
        {
            throw new ArgumentException("Invalid activity canonical state: activity-current-canonical-invalid-json.", nameof(envelope), exception);
        }
    }

    /// <summary>Checks whether server canonical state contains all required metadata.</summary>
    /// <param name="state">The canonical state.</param>
    /// <returns>Whether the state is valid.</returns>
    private static bool IsCanonicalStateValid(ActivityCanonicalState state) =>
        HasRequiredText(state.Status)
        && HasOptionalText(state.Title)
        && HasOptionalText(state.Details)
        && !string.IsNullOrWhiteSpace(state.AcceptedClientId)
        && Guid.TryParseExact(state.AcceptedOperationId, GuidCompactFormat, out _)
        && !string.IsNullOrWhiteSpace(state.AcceptedVersion)
        && DateTimeOffset.TryParse(
            state.ServerAcceptedUtc,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out _);

    /// <summary>Merges the client patch with the current canonical state and trusted server metadata.</summary>
    /// <param name="input">The client patch.</param>
    /// <param name="current">The current canonical state.</param>
    /// <param name="client">The trusted client identity.</param>
    /// <param name="conflict">The conflict context.</param>
    /// <param name="operation">The operation being accepted.</param>
    /// <param name="serverVersion">The accepted server version.</param>
    /// <returns>The merged canonical state.</returns>
    private static ActivityCanonicalState Merge(
        ActivityPayloadInput input,
        ActivityCanonicalState current,
        ClientIdentity client,
        ConflictContext conflict,
        SyncOperation operation,
        string serverVersion)
    {
        var acceptedUtc = GetAcceptedUtc(conflict, operation);
        return new()
        {
            Status = input.Status,
            Title = input.TitleSpecified ? input.Title : current.Title,
            Details = input.DetailsSpecified ? input.Details : current.Details,
            AcceptedClientId = client.ClientId,
            AcceptedOperationId = operation.OperationId.Value.ToString(GuidCompactFormat),
            AcceptedVersion = serverVersion,
            ServerAcceptedUtc = acceptedUtc.ToString(RoundTripDateTimeFormat, CultureInfo.InvariantCulture),
        };
    }

    /// <summary>Attempts to parse activity input with explicit presence tracking.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="input">The parsed input.</param>
    /// <param name="reasonCode">The stable rejection reason when parsing fails.</param>
    /// <returns>Whether parsing succeeded.</returns>
    private static bool TryReadInputPayload(
        ReadOnlyMemory<byte> payload,
        out ActivityPayloadInput input,
        out string reasonCode)
    {
        input = new();
        reasonCode = string.Empty;
        try
        {
            using var document = JsonDocument.Parse(payload);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                reasonCode = "activity-payload-object-required";
                return false;
            }

            return TryReadObject(document.RootElement, out input, out reasonCode);
        }
        catch (JsonException)
        {
            reasonCode = "activity-invalid-json";
            return false;
        }
    }

    /// <summary>Attempts to read an activity object with strict property names and duplicate detection.</summary>
    /// <param name="root">The root JSON object.</param>
    /// <param name="input">The parsed input.</param>
    /// <param name="reasonCode">The stable rejection reason when parsing fails.</param>
    /// <returns>Whether parsing succeeded.</returns>
    private static bool TryReadObject(
        JsonElement root,
        out ActivityPayloadInput input,
        out string reasonCode)
    {
        var parser = new ActivityInputParserState();
        foreach (var property in root.EnumerateObject())
        {
            if (parser.TryReadProperty(property, out reasonCode))
            {
                continue;
            }

            input = new();
            return false;
        }

        input = parser.CreateInput();
        return ValidateInput(input, out reasonCode);
    }

    /// <summary>Validates an activity input object.</summary>
    /// <param name="input">The input object.</param>
    /// <param name="reasonCode">The stable rejection reason when validation fails.</param>
    /// <returns>Whether the input object is valid.</returns>
    private static bool ValidateInput(ActivityPayloadInput input, out string reasonCode)
    {
        if (!HasRequiredText(input.Status))
        {
            reasonCode = "activity-status-required";
            return false;
        }

        if (!HasOptionalText(input.Title) || !HasOptionalText(input.Details))
        {
            reasonCode = "activity-text-too-long";
            return false;
        }

        reasonCode = string.Empty;
        return true;
    }

    /// <summary>Gets the trusted server acceptance timestamp for the operation.</summary>
    /// <param name="conflict">The conflict context.</param>
    /// <param name="operation">The accepted operation.</param>
    /// <returns>The server acceptance timestamp.</returns>
    /// <exception cref="InvalidOperationException">Trusted server provenance is unavailable for the accepted operation.</exception>
    private static DateTimeOffset GetAcceptedUtc(ConflictContext conflict, SyncOperation operation)
    {
        if (conflict.Server is { CandidateWrite: var write }
            && write.OperationId == operation.OperationId
            && string.Equals(write.ClientId, conflict.Client.ClientId, StringComparison.Ordinal))
        {
            return write.CommittedAtUtc;
        }

        throw new InvalidOperationException("Activity canonical state requires trusted server write provenance.");
    }

    /// <summary>Tracks parsed activity input state and duplicate property detection.</summary>
    private sealed class ActivityInputParserState
    {
        /// <summary>The activity status JSON property name.</summary>
        private const string StatusPropertyName = "status";

        /// <summary>The activity title JSON property name.</summary>
        private const string TitlePropertyName = "title";

        /// <summary>The activity details JSON property name.</summary>
        private const string DetailsPropertyName = "details";

        /// <summary>The parsed status.</summary>
        private string _status = string.Empty;

        /// <summary>The parsed title.</summary>
        private string? _title;

        /// <summary>The parsed details.</summary>
        private string? _details;

        /// <summary>Whether title was supplied.</summary>
        private bool _titleSpecified;

        /// <summary>Whether details were supplied.</summary>
        private bool _detailsSpecified;

        /// <summary>Whether status was supplied.</summary>
        private bool _statusSpecified;

        /// <summary>Creates the parsed activity input.</summary>
        /// <returns>The parsed input.</returns>
        internal ActivityPayloadInput CreateInput() =>
            new() { Status = _status, Title = _title, TitleSpecified = _titleSpecified, Details = _details, DetailsSpecified = _detailsSpecified };

        /// <summary>Attempts to read one activity property.</summary>
        /// <param name="property">The JSON property.</param>
        /// <param name="reasonCode">The stable rejection reason when parsing fails.</param>
        /// <returns>Whether the property is valid.</returns>
        internal bool TryReadProperty(JsonProperty property, out string reasonCode)
        {
            switch (property.Name)
            {
                case StatusPropertyName:
                {
                    return TryReadRequiredString(property, out reasonCode);
                }

                case TitlePropertyName:
                {
                    return TryReadOptionalTitle(property, out reasonCode);
                }

                case DetailsPropertyName:
                {
                    return TryReadOptionalDetails(property, out reasonCode);
                }

                default:
                {
                    reasonCode = "activity-unknown-field";
                    return false;
                }
            }
        }

        /// <summary>Reads the required status property.</summary>
        /// <param name="property">The JSON property.</param>
        /// <param name="reasonCode">The stable rejection reason when parsing fails.</param>
        /// <returns>Whether the property is valid.</returns>
        private bool TryReadRequiredString(JsonProperty property, out string reasonCode)
        {
            if (_statusSpecified)
            {
                reasonCode = "activity-status-duplicate";
                return false;
            }

            _statusSpecified = true;
            if (property.Value.ValueKind != JsonValueKind.String)
            {
                reasonCode = "activity-status-type";
                return false;
            }

            var status = property.Value.GetString();
            ArgumentNullException.ThrowIfNull(status);
            _status = status;
            reasonCode = string.Empty;
            return true;
        }

        /// <summary>Reads the optional title property.</summary>
        /// <param name="property">The JSON property.</param>
        /// <param name="reasonCode">The stable rejection reason when parsing fails.</param>
        /// <returns>Whether the property is valid.</returns>
        private bool TryReadOptionalTitle(JsonProperty property, out string reasonCode)
        {
            if (_titleSpecified)
            {
                reasonCode = "activity-title-duplicate";
                return false;
            }

            _titleSpecified = true;
            if (property.Value.ValueKind == JsonValueKind.Null)
            {
                _title = null;
                reasonCode = string.Empty;
                return true;
            }

            if (property.Value.ValueKind != JsonValueKind.String)
            {
                reasonCode = "activity-field-type";
                return false;
            }

            _title = property.Value.GetString();
            reasonCode = string.Empty;
            return true;
        }

        /// <summary>Reads the optional details property.</summary>
        /// <param name="property">The JSON property.</param>
        /// <param name="reasonCode">The stable rejection reason when parsing fails.</param>
        /// <returns>Whether the property is valid.</returns>
        private bool TryReadOptionalDetails(JsonProperty property, out string reasonCode)
        {
            if (_detailsSpecified)
            {
                reasonCode = "activity-details-duplicate";
                return false;
            }

            _detailsSpecified = true;
            if (property.Value.ValueKind == JsonValueKind.Null)
            {
                _details = null;
                reasonCode = string.Empty;
                return true;
            }

            if (property.Value.ValueKind != JsonValueKind.String)
            {
                reasonCode = "activity-field-type";
                return false;
            }

            _details = property.Value.GetString();
            reasonCode = string.Empty;
            return true;
        }
    }
}
