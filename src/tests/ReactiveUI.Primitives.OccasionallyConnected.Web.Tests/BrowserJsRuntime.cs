// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using Microsoft.JSInterop;

namespace ReactiveUI.Primitives.OccasionallyConnected.Web.Tests;

/// <summary>A controlled runtime for listener initialization tests.</summary>
internal sealed class BrowserJsRuntime : IJSRuntime, IAsyncDisposable
{
    /// <summary>The serialization members required by JS interop.</summary>
    private const DynamicallyAccessedMemberTypes JsonSerialized =
        DynamicallyAccessedMemberTypes.PublicConstructors
        | DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties;

    /// <summary>Gets the imported module.</summary>
    public BrowserJsModule Module { get; } = new();

    /// <summary>Gets the import path.</summary>
    public string? ImportPath { get; private set; }

    /// <summary>Gets the number of import calls.</summary>
    public int ImportCount { get; private set; }

    /// <summary>Gets or sets whether import fails.</summary>
    public bool FailImport { get; set; }

    /// <inheritdoc/>
    public ValueTask<TValue> InvokeAsync<[DynamicallyAccessedMembers(JsonSerialized)] TValue>(string identifier, object?[]? args)
    {
        try
        {
            return Import<TValue>(args, CancellationToken.None);
        }
        catch (Exception error)
        {
            return ValueTask.FromException<TValue>(error);
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
            return Import<TValue>(args, cancellationToken);
        }
        catch (Exception error)
        {
            return ValueTask.FromException<TValue>(error);
        }
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        try
        {
            return Module.DisposeAsync();
        }
        catch (Exception error)
        {
            return ValueTask.FromException(error);
        }
    }

    /// <summary>Handles a module import.</summary>
    /// <typeparam name="TValue">The interop result type.</typeparam>
    /// <param name="args">The import arguments.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The module reference.</returns>
    /// <exception cref="JSException">Import is configured to fail.</exception>
    private ValueTask<TValue> Import<TValue>(object?[]? args, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ImportCount++;
        ImportPath = args?[0] as string;
        if (FailImport)
        {
            throw new JSException("Controlled import failure.");
        }

        return ValueTask.FromResult((TValue)(object)Module);
    }
}
