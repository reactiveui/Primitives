// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using Microsoft.JSInterop;

namespace ReactiveUI.Primitives.OccasionallyConnected.Web.Tests;

/// <summary>A controlled browser listener reference.</summary>
internal sealed class BrowserJsListeners : IJSObjectReference
{
    /// <summary>The serialization members required by JS interop.</summary>
    private const DynamicallyAccessedMemberTypes JsonSerialized =
        DynamicallyAccessedMemberTypes.PublicConstructors
        | DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties;

    /// <summary>Gets the number of listener removal calls.</summary>
    public int RemoveCount { get; private set; }

    /// <summary>Gets the number of disposal calls.</summary>
    public int DisposeCount { get; private set; }

    /// <summary>Gets or sets whether the circuit has disconnected.</summary>
    public bool Disconnected { get; set; }

    /// <summary>Gets or sets whether disposal encounters a departed circuit.</summary>
    public bool DisconnectOnDispose { get; set; }

    /// <inheritdoc/>
    public ValueTask<TValue> InvokeAsync<[DynamicallyAccessedMembers(JsonSerialized)] TValue>(string identifier, object?[]? args)
    {
        try
        {
            return Remove<TValue>();
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
            return Remove<TValue>();
        }
        catch (Exception error)
        {
            return ValueTask.FromException<TValue>(error);
        }
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        DisposeCount++;
        return DisconnectOnDispose
            ? ValueTask.FromException(new JSDisconnectedException("Controlled listener disconnect."))
            : ValueTask.CompletedTask;
    }

    /// <summary>Removes this listener set.</summary>
    /// <exception cref="JSDisconnectedException">The circuit is configured to be disconnected.</exception>
    internal void Unregister()
    {
        RemoveCount++;
        if (Disconnected)
        {
            throw new JSDisconnectedException("Controlled circuit disconnect.");
        }
    }

    /// <summary>Handles listener removal.</summary>
    /// <typeparam name="TValue">The result type.</typeparam>
    /// <returns>The interop result.</returns>
    /// <exception cref="JSDisconnectedException">The circuit is configured to be disconnected.</exception>
    private ValueTask<TValue> Remove<TValue>()
    {
        try
        {
            Unregister();
            return ValueTask.FromResult(default(TValue)!);
        }
        catch (Exception error)
        {
            return ValueTask.FromException<TValue>(error);
        }
    }
}
