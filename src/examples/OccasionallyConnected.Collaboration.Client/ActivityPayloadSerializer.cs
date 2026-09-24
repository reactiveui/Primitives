// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Serializes activity payloads in the server example's custom JSON contract.</summary>
internal sealed class ActivityPayloadSerializer : IPayloadSerializer, IOccasionallyConnectedInputCapture<ActivityUpdate>
{
    /// <summary>The maximum activity payload bytes accepted by the server example.</summary>
    public const int MaximumPayloadBytes = 4096;

    /// <summary>The maximum retained typed input byte declaration used by the client example.</summary>
    public const long MaximumRetainedInputBytes = 4096;

    /// <summary>The maximum activity text length accepted by the server example.</summary>
    public const int MaximumTextLength = 256;

    /// <summary>Gets the shared serializer instance.</summary>
    public static ActivityPayloadSerializer Instance { get; } = new();

    /// <inheritdoc />
    public string ContentType => ActivityContracts.ContentType;

    /// <inheritdoc />
    public ValueTask<PayloadEnvelope> SerializeAsync<T>(
        string contractId,
        int schemaVersion,
        T value,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ValidateSchema(contractId, schemaVersion);
        var envelope = value switch
        {
            ActivityUpdate update => CreateEnvelope(update),
            ActivityView view => CreateEnvelope(view),
            _ => throw new InvalidOperationException("The collaboration client can only serialize activity payloads."),
        };
        return ValueTask.FromResult(envelope);
    }

    /// <inheritdoc />
    public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(targetType);
        var update = ReadUpdate(envelope);
        if (targetType == typeof(ActivityUpdate))
        {
            return ValueTask.FromResult<object>(update);
        }

        if (targetType == typeof(ActivityView))
        {
            return ValueTask.FromResult<object>(ActivityView.FromRemote(update, ActivityView.Empty()));
        }

        throw new InvalidOperationException("The collaboration client can only deserialize activity payloads.");
    }

    /// <inheritdoc />
    public long GetRetainedByteCount(ActivityUpdate value)
    {
        ArgumentNullException.ThrowIfNull(value);
        ValidateActivityUpdate(value);
        return MaximumRetainedInputBytes;
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public PayloadEnvelope Capture(ActivityUpdate value) => CreateEnvelope(value);

    /// <summary>Attempts to read a canonical activity view from an envelope.</summary>
    /// <param name="envelope">The envelope to read.</param>
    /// <param name="view">The decoded view when the read succeeds.</param>
    /// <returns>Whether the envelope could be read as an activity view.</returns>
    internal static bool TryReadView(PayloadEnvelope envelope, out ActivityView view)
    {
        try
        {
            view = ActivityView.FromRemote(ReadUpdate(envelope), ActivityView.Empty());
            return true;
        }
        catch (ArgumentException)
        {
            view = ActivityView.Empty();
            return false;
        }
        catch (InvalidOperationException)
        {
            view = ActivityView.Empty();
            return false;
        }
    }

    /// <summary>Creates an envelope for an activity update.</summary>
    /// <param name="update">The update.</param>
    /// <returns>The encoded envelope.</returns>
    internal static PayloadEnvelope CreateEnvelope(ActivityUpdate update)
    {
        ArgumentNullException.ThrowIfNull(update);
        ValidateActivityUpdate(update);
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteString("status", update.Status);
            if (update.TitleSpecified)
            {
                WriteOptionalString(writer, "title", update.Title);
            }

            if (update.DetailsSpecified)
            {
                WriteOptionalString(writer, "details", update.Details);
            }

            writer.WriteEndObject();
        }

        return CreateEnvelope(stream.ToArray());
    }

    /// <summary>Creates an envelope for an activity view.</summary>
    /// <param name="view">The view.</param>
    /// <returns>The encoded envelope.</returns>
    internal static PayloadEnvelope CreateEnvelope(ActivityView view)
    {
        ArgumentNullException.ThrowIfNull(view);
        ValidateActivityView(view);
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteString("status", view.Status);
            WriteOptionalString(writer, "title", view.Title);
            WriteOptionalString(writer, "details", view.Details);
            writer.WriteString("acceptedClientId", view.AcceptedClientId);
            writer.WriteString("acceptedOperationId", view.AcceptedOperationId);
            writer.WriteString("acceptedVersion", view.AcceptedVersion);
            writer.WriteString("serverAcceptedUtc", view.ServerAcceptedUtc);
            writer.WriteEndObject();
        }

        return CreateEnvelope(stream.ToArray());
    }

    /// <summary>Reads an activity update from an envelope.</summary>
    /// <param name="envelope">The envelope.</param>
    /// <returns>The decoded update.</returns>
    /// <exception cref="ArgumentException">The envelope does not contain a valid activity payload.</exception>
    internal static ActivityUpdate ReadUpdate(PayloadEnvelope envelope)
    {
        ValidateEnvelope(envelope);
        using var document = JsonDocument.Parse(envelope.Payload);
        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("Activity payloads must be JSON objects.", nameof(envelope));
        }

        var parser = new ActivityUpdateParser();
        foreach (var property in document.RootElement.EnumerateObject())
        {
            parser.Read(property);
        }

        var update = parser.Create();
        ValidateActivityUpdate(update);
        return update;
    }

    /// <summary>Creates an envelope from encoded JSON bytes.</summary>
    /// <param name="bytes">The JSON bytes.</param>
    /// <returns>The payload envelope.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The encoded payload length is outside the supported range.</exception>
    private static PayloadEnvelope CreateEnvelope(byte[] bytes)
    {
        if (bytes.Length is <= 0 or > MaximumPayloadBytes)
        {
            throw new ArgumentOutOfRangeException(nameof(bytes), bytes.Length, "Activity payload bytes must fit the server contract.");
        }

        return new(
            ActivityContracts.ContractId,
            ActivityContracts.SchemaVersion,
            ActivityContracts.ContentType,
            bytes,
            CreateHash(bytes));
    }

    /// <summary>Creates the SHA-256 payload hash.</summary>
    /// <param name="bytes">The bytes to hash.</param>
    /// <returns>The encoded hash.</returns>
    private static string CreateHash(byte[] bytes)
    {
#if NET8_0_OR_GREATER
        var hash = SHA256.HashData(bytes);
#else
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(bytes);
#endif
        return $"sha256-{Convert.ToBase64String(hash)}";
    }

    /// <summary>Writes an optional string JSON property.</summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The optional value.</param>
    private static void WriteOptionalString(Utf8JsonWriter writer, string propertyName, string? value)
    {
        if (value is null)
        {
            writer.WriteNull(propertyName);
            return;
        }

        writer.WriteString(propertyName, value);
    }

    /// <summary>Validates an envelope before JSON parsing.</summary>
    /// <param name="envelope">The envelope.</param>
    /// <exception cref="ArgumentException">The envelope does not match the activity contract.</exception>
    private static void ValidateEnvelope(PayloadEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ValidateSchema(envelope.ContractId, envelope.SchemaVersion);
        if (!string.Equals(envelope.ContentType, ActivityContracts.ContentType, StringComparison.Ordinal))
        {
            throw new ArgumentException("Activity content type does not match the collaboration server contract.", nameof(envelope));
        }

        if (envelope.PayloadLength is <= 0 or > MaximumPayloadBytes)
        {
            throw new ArgumentException("Activity payload size does not fit the collaboration server contract.", nameof(envelope));
        }

        var payload = envelope.Payload.ToArray();
        var expected = Encoding.UTF8.GetBytes(CreateHash(payload));
        var actual = Encoding.UTF8.GetBytes(envelope.PayloadHash);
        if (!CryptographicOperations.FixedTimeEquals(actual, expected))
        {
            throw new ArgumentException("Activity payload hash does not match the encoded bytes.", nameof(envelope));
        }
    }

    /// <summary>Validates the activity contract metadata.</summary>
    /// <param name="contractId">The contract id.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <exception cref="InvalidOperationException">The schema does not match the activity contract.</exception>
    private static void ValidateSchema(string contractId, int schemaVersion)
    {
        if (!string.Equals(contractId, ActivityContracts.ContractId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Activity contract id does not match the collaboration server contract.");
        }

        if (schemaVersion != ActivityContracts.SchemaVersion)
        {
            throw new InvalidOperationException("Activity schema version does not match the collaboration server contract.");
        }
    }

    /// <summary>Validates an activity update.</summary>
    /// <param name="update">The update.</param>
    private static void ValidateActivityUpdate(ActivityUpdate update)
    {
        ValidateRequiredText(update.Status, nameof(update.Status));
        ValidateOptionalText(update.Title, nameof(update.Title));
        ValidateOptionalText(update.Details, nameof(update.Details));
    }

    /// <summary>Validates an activity view.</summary>
    /// <param name="view">The view.</param>
    /// <exception cref="ArgumentException">The activity view does not match the activity contract.</exception>
    private static void ValidateActivityView(ActivityView view)
    {
        ValidateRequiredText(view.Status, nameof(view.Status));
        ValidateOptionalText(view.Title, nameof(view.Title));
        ValidateOptionalText(view.Details, nameof(view.Details));
        ValidateRequiredText(view.AcceptedClientId, nameof(view.AcceptedClientId));
        ValidateRequiredText(view.AcceptedVersion, nameof(view.AcceptedVersion));
        if (!Guid.TryParseExact(view.AcceptedOperationId, "N", out _))
        {
            throw new ArgumentException("Activity operation id must use compact GUID format.", nameof(view));
        }

        if (!DateTimeOffset.TryParse(
                view.ServerAcceptedUtc,
                CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind,
                out _))
        {
            throw new ArgumentException("Activity accepted timestamp must use round-trip date/time format.", nameof(view));
        }
    }

    /// <summary>Validates required text.</summary>
    /// <param name="value">The value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The value is missing or too long.</exception>
    private static void ValidateRequiredText(string? value, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > MaximumTextLength)
        {
            throw new ArgumentException("Activity text must be present and no more than 256 characters.", parameterName);
        }
    }

    /// <summary>Validates optional text.</summary>
    /// <param name="value">The value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentException">The value is too long.</exception>
    private static void ValidateOptionalText(string? value, string parameterName)
    {
        if (value is not null && value.Length > MaximumTextLength)
        {
            throw new ArgumentException("Activity text must be no more than 256 characters.", parameterName);
        }
    }

    /// <summary>Tracks explicitly present activity JSON fields.</summary>
    private sealed class ActivityUpdateParser
    {
        /// <summary>Stores whether the status was read.</summary>
        private bool _statusSpecified;

        /// <summary>Stores the decoded status.</summary>
        private string? _status;

        /// <summary>Stores whether the title was read.</summary>
        private bool _titleSpecified;

        /// <summary>Stores the decoded title.</summary>
        private string? _title;

        /// <summary>Stores whether the details were read.</summary>
        private bool _detailsSpecified;

        /// <summary>Stores the decoded details.</summary>
        private string? _details;

        /// <summary>Stores the accepted client id.</summary>
        private string? _acceptedClientId;

        /// <summary>Stores the accepted operation id.</summary>
        private string? _acceptedOperationId;

        /// <summary>Stores the accepted version.</summary>
        private string? _acceptedVersion;

        /// <summary>Stores the accepted timestamp.</summary>
        private string? _serverAcceptedUtc;

        /// <summary>Reads one JSON property.</summary>
        /// <param name="property">The property.</param>
        /// <exception cref="ArgumentException">The property is duplicated or unsupported.</exception>
        internal void Read(JsonProperty property)
        {
            switch (property.Name)
            {
                case "status":
                {
                    RejectDuplicate(_statusSpecified, property.Name);
                    _statusSpecified = true;
                    _status = ReadRequiredString(property);
                    break;
                }

                case "title":
                {
                    RejectDuplicate(_titleSpecified, property.Name);
                    _titleSpecified = true;
                    _title = ReadNullableString(property);
                    break;
                }

                case "details":
                {
                    RejectDuplicate(_detailsSpecified, property.Name);
                    _detailsSpecified = true;
                    _details = ReadNullableString(property);
                    break;
                }

                case "acceptedClientId":
                {
                    _acceptedClientId = ReadRequiredString(property);
                    break;
                }

                case "acceptedOperationId":
                {
                    _acceptedOperationId = ReadRequiredString(property);
                    break;
                }

                case "acceptedVersion":
                {
                    _acceptedVersion = ReadRequiredString(property);
                    break;
                }

                case "serverAcceptedUtc":
                {
                    _serverAcceptedUtc = ReadRequiredString(property);
                    break;
                }

                default:
                    throw new ArgumentException("Activity payload contains an unknown property.", property.Name);
            }
        }

        /// <summary>Creates the decoded update.</summary>
        /// <returns>The activity update.</returns>
        internal ActivityUpdate Create() =>
            new()
            {
                Status = _status ?? string.Empty,
                Title = _title,
                TitleSpecified = _titleSpecified,
                Details = _details,
                DetailsSpecified = _detailsSpecified,
                AcceptedClientId = _acceptedClientId,
                AcceptedOperationId = _acceptedOperationId,
                AcceptedVersion = _acceptedVersion,
                ServerAcceptedUtc = _serverAcceptedUtc,
            };

        /// <summary>Rejects duplicate JSON fields.</summary>
        /// <param name="seen">Whether the field was already seen.</param>
        /// <param name="propertyName">The property name.</param>
        /// <exception cref="ArgumentException">The property was already seen.</exception>
        private static void RejectDuplicate(bool seen, string propertyName)
        {
            if (seen)
            {
                throw new ArgumentException("Activity payload contains a duplicate property.", propertyName);
            }
        }

        /// <summary>Reads a required JSON string property.</summary>
        /// <param name="property">The property.</param>
        /// <returns>The decoded string.</returns>
        /// <exception cref="ArgumentException">The property is not a string.</exception>
        private static string ReadRequiredString(JsonProperty property) =>
            property.Value.ValueKind == JsonValueKind.String
                ? property.Value.GetString() ?? string.Empty
                : throw new ArgumentException("Activity payload property must be a string.", property.Name);

        /// <summary>Reads a nullable JSON string property.</summary>
        /// <param name="property">The property.</param>
        /// <returns>The decoded string.</returns>
        /// <exception cref="ArgumentException">The property is not a string or null.</exception>
        private static string? ReadNullableString(JsonProperty property) =>
            property.Value.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.String => property.Value.GetString(),
                _ => throw new ArgumentException("Activity payload property must be a string or null.", property.Name),
            };
    }
}
