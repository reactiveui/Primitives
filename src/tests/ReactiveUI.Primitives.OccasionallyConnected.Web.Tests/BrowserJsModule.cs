// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using Microsoft.JSInterop;

namespace ReactiveUI.Primitives.OccasionallyConnected.Web.Tests;

/// <summary>A controlled browser module reference.</summary>
internal sealed class BrowserJsModule : IJSObjectReference
{
    /// <summary>The serialization members required by JS interop.</summary>
    private const DynamicallyAccessedMemberTypes JsonSerialized =
        DynamicallyAccessedMemberTypes.PublicConstructors
        | DynamicallyAccessedMemberTypes.PublicFields
        | DynamicallyAccessedMemberTypes.PublicProperties;

    /// <summary>Gets the listener reference.</summary>
    public BrowserJsListeners Listeners { get; } = new();

    /// <summary>Gets the number of listener registrations.</summary>
    public int ObserveCount { get; private set; }

    /// <summary>Gets the number of disposal calls.</summary>
    public int DisposeCount { get; private set; }

    /// <summary>Gets or sets whether listener registration is cancelled after import.</summary>
    public bool CancelObserve { get; set; }

    /// <summary>Gets or sets whether module disposal encounters a departed circuit.</summary>
    public bool DisconnectOnDispose { get; set; }

    /// <inheritdoc/>
    public ValueTask<TValue> InvokeAsync<[DynamicallyAccessedMembers(JsonSerialized)] TValue>(string identifier, object?[]? args)
    {
        try
        {
            return Observe<TValue>(identifier, CancellationToken.None);
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
            return Observe<TValue>(identifier, cancellationToken);
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
            ? ValueTask.FromException(new JSDisconnectedException("Controlled module disconnect."))
            : ValueTask.CompletedTask;
    }

    /// <summary>Handles a listener registration.</summary>
    /// <typeparam name="TValue">The result type.</typeparam>
    /// <param name="identifier">The module function name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The listener reference.</returns>
    private ValueTask<TValue> Observe<TValue>(string identifier, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (string.Equals(identifier, "unobserve", StringComparison.Ordinal))
        {
            try
            {
                Listeners.Unregister();
                return ValueTask.FromResult(default(TValue)!);
            }
            catch (Exception error)
            {
                return ValueTask.FromException<TValue>(error);
            }
        }

        ObserveCount++;
        return CancelObserve
            ? ValueTask.FromCanceled<TValue>(new(canceled: true))
            : ValueTask.FromResult((TValue)(object)Listeners);
    }
}
