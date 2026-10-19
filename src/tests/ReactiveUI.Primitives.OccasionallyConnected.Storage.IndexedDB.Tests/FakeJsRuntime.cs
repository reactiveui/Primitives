// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.JSInterop;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB.Tests;

/// <summary>Provides a fake JS runtime for IndexedDB adapter tests.</summary>
internal sealed class FakeJsRuntime : IJSRuntime
{
    /// <summary>Gets the linker flags used by JS interop JSON serialization.</summary>
    private const DynamicallyAccessedMemberTypes JsonSerialized =
        DynamicallyAccessedMemberTypes.PublicConstructors
        | DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties;

    /// <summary>The fake imported module instance.</summary>
    private readonly FakeJsModule _module = new();

    /// <summary>Gets the imported module paths.</summary>
    public List<string> Imports { get; } = [];

    /// <summary>Gets the imported fake module.</summary>
    public FakeJsModule Module => _module;

    /// <inheritdoc/>
    public ValueTask<TValue> InvokeAsync<[DynamicallyAccessedMembers(JsonSerialized)] TValue>(string identifier, object?[]? args)
    {
        try
        {
            if (!string.Equals(identifier, "import", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected JS runtime call: {identifier}.");
            }

            var path = (args?.Length > 0 ? args[0] as string : null) ?? throw new InvalidOperationException("Import path is required.");
            Imports.Add(path);
            return new((TValue)(object)_module);
        }
        catch (Exception error)
        {
            throw new InvalidOperationException("The fake JS runtime failed to handle the invocation.", error);
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
            if (!string.Equals(identifier, "import", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unexpected JS runtime call: {identifier}.");
            }

            var path = (args?.Length > 0 ? args[0] as string : null) ?? throw new InvalidOperationException("Import path is required.");
            Imports.Add(path);
            return new((TValue)(object)_module);
        }
        catch (Exception error)
        {
            throw new InvalidOperationException("The fake JS runtime failed to handle the invocation.", error);
        }
    }
}
