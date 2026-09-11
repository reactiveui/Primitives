// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores allowlisted JSON payload schemas and upcasters.</summary>
[DebuggerDisplay("SchemaRegistry")]
public sealed class SchemaRegistry : ISchemaRegistry
{
    /// <summary>The message used when a contract identifier is empty.</summary>
    private const string EmptyContractMessage = "The payload contract cannot be empty.";

    /// <summary>The message used when a schema version is not positive.</summary>
    private const string PositiveSchemaVersionMessage = "The payload schema version must be positive.";

    /// <summary>Stores registered schemas by contract, version, and target type.</summary>
    private readonly Dictionary<SchemaKey, JsonTypeInfo> _schemas = [];

    /// <summary>Stores the highest registered schema version for each contract and target type.</summary>
    private readonly Dictionary<TargetKey, int> _targetVersions = [];

    /// <summary>Stores whether a contract identifier has at least one registered schema.</summary>
    private readonly HashSet<string> _contracts = [];

    /// <summary>Stores registered upcasters in insertion order.</summary>
    private readonly List<IPayloadUpcaster> _upcasters = [];

    /// <summary>Registers a payload schema.</summary>
    /// <typeparam name="T">The allowlisted payload type.</typeparam>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="schemaVersion">The positive schema version.</param>
    /// <param name="jsonTypeInfo">The source-generated JSON metadata.</param>
    /// <returns>The current registry.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="contractId"/> or <paramref name="jsonTypeInfo"/> is <see langword="null"/>.</exception>
    /// <exception cref="PayloadSchemaException"><paramref name="schemaVersion"/> is invalid or the schema was already registered.</exception>
    public SchemaRegistry Register<T>(string contractId, int schemaVersion, JsonTypeInfo<T> jsonTypeInfo)
    {
        ArgumentExceptionHelper.ThrowIfNull(contractId);
        ArgumentExceptionHelper.ThrowIfNull(jsonTypeInfo);
        if (contractId.Length == 0)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, EmptyContractMessage);
        }

        if (schemaVersion <= 0)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.InvalidSchemaVersion, PositiveSchemaVersionMessage);
        }

        ValidateJsonTypeInfo(jsonTypeInfo);
        jsonTypeInfo.MakeReadOnly();

        SchemaKey schemaKey = new(contractId, schemaVersion, typeof(T));
        if (_schemas.ContainsKey(schemaKey))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.InvalidSchemaVersion, "The payload schema is already registered.");
        }

        _schemas.Add(schemaKey, jsonTypeInfo);
        _ = _contracts.Add(contractId);
        TargetKey targetKey = new(contractId, typeof(T));
        if (!_targetVersions.TryGetValue(targetKey, out var existingVersion)
            || schemaVersion > existingVersion)
        {
            _targetVersions[targetKey] = schemaVersion;
        }

        return this;
    }

    /// <summary>Registers an upcaster.</summary>
    /// <param name="upcaster">The payload upcaster.</param>
    /// <returns>The current registry.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="upcaster"/> is <see langword="null"/>.</exception>
    /// <exception cref="PayloadSchemaException">The upcaster does not describe a contiguous forward step.</exception>
    public SchemaRegistry RegisterUpcaster(IPayloadUpcaster upcaster)
    {
        ArgumentExceptionHelper.ThrowIfNull(upcaster);
        if (string.IsNullOrEmpty(upcaster.ContractId))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, EmptyContractMessage);
        }

        var fromVersion = upcaster.FromVersion;
        var toVersion = upcaster.ToVersion;
        if (fromVersion <= 0 || toVersion <= 0 || fromVersion == int.MaxValue)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.InvalidSchemaVersion, PositiveSchemaVersionMessage);
        }

        if (toVersion != fromVersion + 1)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.MissingUpcaster, "Upcasters must describe one contiguous forward schema step.");
        }

        _upcasters.Add(new RegisteredPayloadUpcaster(upcaster));
        return this;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRegistered(string contractId, int schemaVersion, Type targetType) =>
        _schemas.ContainsKey(new(contractId, schemaVersion, targetType));

    /// <inheritdoc/>
    /// <exception cref="PayloadSchemaException">No valid contiguous chain exists.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IReadOnlyList<IPayloadUpcaster> GetUpcastChain(string contractId, int fromVersion, int toVersion) =>
        ResolveUpcastChain(contractId, fromVersion, toVersion);

    /// <summary>Gets source-generated JSON metadata for a registered schema.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <param name="targetType">The requested target payload type.</param>
    /// <returns>The registered JSON type metadata.</returns>
    /// <exception cref="PayloadSchemaException">The schema is not allowlisted for the requested type.</exception>
    internal JsonTypeInfo GetJsonTypeInfo(string contractId, int schemaVersion, Type targetType)
    {
        SchemaKey key = new(contractId, schemaVersion, targetType);
        if (_schemas.TryGetValue(key, out var jsonTypeInfo))
        {
            return jsonTypeInfo;
        }

        if (!_contracts.Contains(contractId))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, "The payload contract is not registered.");
        }

        if (schemaVersion <= 0)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.InvalidSchemaVersion, PositiveSchemaVersionMessage);
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, "The target type is not allowlisted for the payload contract and schema version.");
    }

    /// <summary>Gets the registered target schema version for a payload type.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="targetType">The requested target payload type.</param>
    /// <returns>The registered schema version for the target type.</returns>
    /// <exception cref="PayloadSchemaException">The requested target is not registered.</exception>
    internal int GetTargetSchemaVersion(string contractId, Type targetType)
    {
        if (_targetVersions.TryGetValue(new(contractId, targetType), out var schemaVersion))
        {
            return schemaVersion;
        }

        if (!_contracts.Contains(contractId))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, "The payload contract is not registered.");
        }

        throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, "The target type is not allowlisted for the payload contract.");
    }

    /// <summary>Determines whether a contract identifier has been registered.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <returns><see langword="true"/> when the contract is known; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool ContainsContract(string contractId) => _contracts.Contains(contractId);

    /// <summary>Creates an immutable copy of the current registry state.</summary>
    /// <returns>The registry snapshot.</returns>
    internal SchemaRegistry Snapshot()
    {
        SchemaRegistry snapshot = new();
        foreach (var schema in _schemas)
        {
            snapshot._schemas.Add(schema.Key, schema.Value);
        }

        foreach (var targetVersion in _targetVersions)
        {
            snapshot._targetVersions.Add(targetVersion.Key, targetVersion.Value);
        }

        foreach (var contract in _contracts)
        {
            _ = snapshot._contracts.Add(contract);
        }

        snapshot._upcasters.AddRange(_upcasters);
        return snapshot;
    }

    /// <summary>Validates that JSON metadata cannot emit reference or polymorphic metadata.</summary>
    /// <param name="jsonTypeInfo">The JSON metadata to validate.</param>
    /// <exception cref="PayloadSchemaException"><paramref name="jsonTypeInfo"/> enables reference preservation or polymorphism.</exception>
    private static void ValidateJsonTypeInfo(JsonTypeInfo jsonTypeInfo)
    {
        var referenceHandler = jsonTypeInfo.Options.ReferenceHandler;
        if (referenceHandler is null && jsonTypeInfo.PolymorphismOptions is null)
        {
            return;
        }

        var message = referenceHandler is not null
            ? "Payload JSON metadata cannot use reference preservation."
            : "Payload JSON metadata cannot use polymorphic type metadata.";
        throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, message);
    }

    /// <summary>Finds a contiguous upcast chain between two schema versions.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="fromVersion">The current schema version.</param>
    /// <param name="toVersion">The requested schema version.</param>
    /// <returns>The upcasters that convert the payload to the requested schema version.</returns>
    /// <exception cref="PayloadSchemaException">No valid contiguous chain exists.</exception>
    private List<IPayloadUpcaster> ResolveUpcastChain(string contractId, int fromVersion, int toVersion)
    {
        ArgumentExceptionHelper.ThrowIfNull(contractId);
        if (contractId.Length == 0)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.UnknownContract, EmptyContractMessage);
        }

        if (fromVersion <= 0 || toVersion <= 0)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.InvalidSchemaVersion, PositiveSchemaVersionMessage);
        }

        if (fromVersion > toVersion)
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.DowncastNotSupported, "Payload schema downcasting is not supported.");
        }

        if (fromVersion == toVersion)
        {
            return [];
        }

        List<IPayloadUpcaster> chain = new();
        for (var currentVersion = fromVersion; currentVersion < toVersion; currentVersion++)
        {
            chain.Add(FindUpcaster(contractId, currentVersion));
        }

        return chain;
    }

    /// <summary>Finds one upcaster for a contiguous schema step.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="fromVersion">The source schema version.</param>
    /// <returns>The matching upcaster.</returns>
    /// <exception cref="PayloadSchemaException">The step is missing or ambiguous.</exception>
    private IPayloadUpcaster FindUpcaster(string contractId, int fromVersion)
    {
        var matched = default(IPayloadUpcaster);
        for (var index = 0; index < _upcasters.Count; index++)
        {
            var candidate = _upcasters[index];
            if (!string.Equals(candidate.ContractId, contractId, StringComparison.Ordinal)
                || candidate.FromVersion != fromVersion
                || candidate.ToVersion != fromVersion + 1)
            {
                continue;
            }

            if (matched is not null)
            {
                throw new PayloadSchemaException(PayloadSchemaFailureReason.AmbiguousUpcaster, "More than one upcaster handles the same schema step.");
            }

            matched = candidate;
        }

        return matched ?? throw new PayloadSchemaException(PayloadSchemaFailureReason.MissingUpcaster, "A contiguous upcaster is missing.");
    }

    /// <summary>Identifies a registered schema by contract, version, and type.</summary>
    /// <param name="ContractId">The stable wire contract identifier.</param>
    /// <param name="SchemaVersion">The schema version.</param>
    /// <param name="TargetType">The target payload type.</param>
    private readonly record struct SchemaKey(string ContractId, int SchemaVersion, Type TargetType);

    /// <summary>Identifies the target schema version for a contract and type.</summary>
    /// <param name="ContractId">The stable wire contract identifier.</param>
    /// <param name="TargetType">The target payload type.</param>
    private readonly record struct TargetKey(string ContractId, Type TargetType);

    /// <summary>Captures stable upcaster metadata while delegating conversion to the registered upcaster.</summary>
    private sealed class RegisteredPayloadUpcaster : IPayloadUpcaster
    {
        /// <summary>The registered upcaster implementation.</summary>
        private readonly IPayloadUpcaster _inner;

        /// <summary>Initializes a new instance of the <see cref="RegisteredPayloadUpcaster"/> class.</summary>
        /// <param name="inner">The registered upcaster implementation.</param>
        public RegisteredPayloadUpcaster(IPayloadUpcaster inner)
        {
            ContractId = inner.ContractId;
            FromVersion = inner.FromVersion;
            ToVersion = inner.ToVersion;
            _inner = inner;
        }

        /// <inheritdoc/>
        public string ContractId { get; }

        /// <inheritdoc/>
        public int FromVersion { get; }

        /// <inheritdoc/>
        public int ToVersion { get; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken) =>
            _inner.UpcastAsync(source, cancellationToken);
    }
}
