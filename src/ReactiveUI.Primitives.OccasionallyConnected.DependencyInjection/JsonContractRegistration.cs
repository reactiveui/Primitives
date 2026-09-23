// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

/// <summary>Stores a generated JSON contract registration.</summary>
internal sealed class JsonContractRegistration
{
    /// <summary>Stores the schema registration callback.</summary>
    private readonly Action<SchemaRegistry> _register;

    /// <summary>Initializes a new instance of the <see cref="JsonContractRegistration"/> class.</summary>
    /// <param name="register">The schema registration callback.</param>
    private JsonContractRegistration(Action<SchemaRegistry> register) => _register = register;

    /// <summary>Creates a generated JSON contract registration.</summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="contractId">The contract identifier.</param>
    /// <param name="schemaVersion">The schema version.</param>
    /// <param name="jsonTypeInfo">The generated JSON metadata.</param>
    /// <returns>The generated JSON contract registration.</returns>
    /// <exception cref="ArgumentException">The contract identifier is blank.</exception>
    internal static JsonContractRegistration Create<T>(
        string contractId,
        int schemaVersion,
        JsonTypeInfo<T> jsonTypeInfo)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(contractId);
#else
        ArgumentExceptionHelper.ThrowIfNull(contractId);
        if (string.IsNullOrWhiteSpace(contractId))
        {
            throw new ArgumentException("Contract identifier must be supplied.", nameof(contractId));
        }
#endif
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(schemaVersion);
        ArgumentExceptionHelper.ThrowIfNull(jsonTypeInfo);
        return new(registry => _ = registry.Register(contractId, schemaVersion, jsonTypeInfo));
    }

    /// <summary>Registers this contract with the supplied schema registry.</summary>
    /// <param name="registry">The registry to populate.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Register(SchemaRegistry registry) => _register(registry);
}
