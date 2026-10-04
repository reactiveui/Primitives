// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.JSInterop;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB.Tests;

/// <summary>Provides a fake JS object reference for IndexedDB adapter tests.</summary>
internal sealed class FakeJsModule : IJSObjectReference
{
    /// <summary>Gets the linker flags used by JS interop JSON serialization.</summary>
    private const DynamicallyAccessedMemberTypes JsonSerialized =
        DynamicallyAccessedMemberTypes.PublicConstructors
        | DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties;

    /// <summary>The stored compare-exchange records.</summary>
    private readonly Dictionary<string, StoredRecord> _records = [];

    /// <summary>Gets a value indicating whether disposal was requested.</summary>
    public bool DisposeCalled { get; private set; }

    /// <summary>Gets or sets a value indicating whether the next compare-exchange should fail.</summary>
    public bool FailNextCompareExchange { get; set; }

    /// <summary>Gets or sets a corrupted document returned by load calls.</summary>
    public string? LoadedJsonOverride { get; set; }

    /// <summary>Gets the invoked module methods.</summary>
    public List<string> Invocations { get; } = [];

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        DisposeCalled = true;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask<TValue> InvokeAsync<[DynamicallyAccessedMembers(JsonSerialized)] TValue>(string identifier, object?[]? args)
    {
        try
        {
            return InvokeCore<TValue>(identifier, args);
        }
        catch (Exception error)
        {
            throw new InvalidOperationException("The fake JS module failed to handle the invocation.", error);
        }
    }

    /// <inheritdoc/>
    public ValueTask<TValue> InvokeAsync<[DynamicallyAccessedMembers(JsonSerialized)] TValue>(
        string identifier,
        CancellationToken cancellationToken,
        object?[]? args)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return InvokeCore<TValue>(identifier, args);
        }
        catch (Exception error)
        {
            throw new InvalidOperationException("The fake JS module failed to handle the invocation.", error);
        }
    }

    /// <summary>Handles one fake module invocation.</summary>
    /// <typeparam name="TValue">The JS return type.</typeparam>
    /// <param name="identifier">The JS identifier.</param>
    /// <param name="args">The JS arguments.</param>
    /// <returns>The invocation result.</returns>
    /// <exception cref="InvalidOperationException">The identifier is not supported or required arguments are missing.</exception>
    private ValueTask<TValue> InvokeCore<[DynamicallyAccessedMembers(JsonSerialized)] TValue>(string identifier, object?[]? args)
    {
        Invocations.Add(identifier);
        if (string.Equals(identifier, "loadStore", StringComparison.Ordinal))
        {
            try
            {
                return LoadStore<TValue>(args);
            }
            catch (Exception error)
            {
                throw new InvalidOperationException("The fake JS module failed to load store state.", error);
            }
        }

        if (string.Equals(identifier, "compareExchangeStore", StringComparison.Ordinal))
        {
            try
            {
                return CompareExchangeStore<TValue>(args);
            }
            catch (Exception error)
            {
                throw new InvalidOperationException("The fake JS module failed to compare and exchange store state.", error);
            }
        }

        throw new InvalidOperationException($"Unexpected JS module call: {identifier}.");
    }

    /// <summary>Loads one stored JSON document.</summary>
    /// <typeparam name="TValue">The JS return type.</typeparam>
    /// <param name="args">The JS invocation arguments.</param>
    /// <returns>The loaded JSON value task.</returns>
    private ValueTask<TValue> LoadStore<TValue>(object?[]? args)
    {
        var databaseName = (string?)args?[0] ?? string.Empty;
        var objectStoreName = (string?)args?[1] ?? string.Empty;
        var key = (string?)args?[2] ?? string.Empty;
        var compositeKey = $"{databaseName}|{objectStoreName}|{key}";
        var value = LoadedJsonOverride ?? (_records.TryGetValue(compositeKey, out var stored) ? stored.Json : null);
        return value is null ? new(default(TValue)!) : new((TValue)(object)value);
    }

    /// <summary>Applies one compare-exchange write.</summary>
    /// <typeparam name="TValue">The JS return type.</typeparam>
    /// <param name="args">The JS invocation arguments.</param>
    /// <returns>The compare-exchange result.</returns>
    /// <exception cref="InvalidOperationException">A JSON document was not supplied.</exception>
    private ValueTask<TValue> CompareExchangeStore<TValue>(object?[]? args)
    {
        try
        {
            return new((TValue)(object)TryCompareExchange(args));
        }
        catch (Exception error)
        {
            throw new InvalidOperationException("The fake JS module failed to compare and exchange store state.", error);
        }
    }

    /// <summary>Attempts one compare-exchange update against the fake store.</summary>
    /// <param name="args">The JS invocation arguments.</param>
    /// <returns>True when the record is updated; otherwise false.</returns>
    /// <exception cref="InvalidOperationException">A JSON document was not supplied.</exception>
    private bool TryCompareExchange(object?[]? args)
    {
        var shouldFail = FailNextCompareExchange;
        FailNextCompareExchange = false;
        if (shouldFail)
        {
            return false;
        }

        try
        {
            var (compositeKey, expectedGeneration, json) = ParseArguments(args);
            return TryUpdateRecordCore(compositeKey, expectedGeneration, json);
        }
        catch (Exception error)
        {
            throw new InvalidOperationException("The fake JS module failed to parse or update compare-exchange state.", error);
        }

        static (string CompositeKey, long ExpectedGeneration, string Json) ParseArguments(object?[]? values)
        {
            var databaseName = (string?)values?[0] ?? string.Empty;
            var objectStoreName = (string?)values?[1] ?? string.Empty;
            var key = (string?)values?[2] ?? string.Empty;
            var expected = Convert.ToInt64(values![3], System.Globalization.CultureInfo.InvariantCulture);
            var payload = (string?)values[4] ?? throw new InvalidOperationException("A JSON document is required.");
            return ($"{databaseName}|{objectStoreName}|{key}", expected, payload);
        }

        bool TryUpdateRecordCore(string compositeKeyValue, long expectedGenerationValue, string jsonValue)
        {
            var currentGeneration = _records.TryGetValue(compositeKeyValue, out var record) ? record.Generation : 0;
            if (currentGeneration != expectedGenerationValue)
            {
                return false;
            }

            _records[compositeKeyValue] = new(expectedGenerationValue + 1, jsonValue);
            return true;
        }
    }

    /// <summary>Represents one stored compare-exchange record.</summary>
    /// <param name="Generation">The current record generation.</param>
    /// <param name="Json">The stored JSON document.</param>
    private sealed record StoredRecord(long Generation, string Json);
}
