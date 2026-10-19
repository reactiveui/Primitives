// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using Microsoft.Maui.Networking;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Adapts the MAUI Essentials connectivity service without treating its hints as delivery guarantees.</summary>
[DebuggerDisplay("NetworkAvailable = {NetworkAvailable}")]
public sealed class MauiConnectivityHint : IMobileConnectivityHint, IDisposable
{
    /// <summary>The platform connectivity service.</summary>
    private readonly IConnectivity _connectivity;

    /// <summary>Whether the subscription was removed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="MauiConnectivityHint"/> class.</summary>
    /// <param name="connectivity">The platform's Essentials connectivity service.</param>
    /// <exception cref="ArgumentNullException"><paramref name="connectivity"/> is null.</exception>
    public MauiConnectivityHint(IConnectivity connectivity)
    {
        ArgumentNullException.ThrowIfNull(connectivity);
        _connectivity = connectivity;
        connectivity.ConnectivityChanged += OnChanged;
    }

    /// <inheritdoc/>
    public event EventHandler? Changed;

    /// <inheritdoc/>
    public bool NetworkAvailable => _connectivity.NetworkAccess == NetworkAccess.Internet;

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            _connectivity.ConnectivityChanged -= OnChanged;
        }
    }

    /// <summary>Forwards a connectivity hint.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="args">The changed connectivity details.</param>
    private void OnChanged(object? sender, ConnectivityChangedEventArgs args)
    {
        if (Volatile.Read(ref _disposed) == 0)
        {
            Changed?.Invoke(this, EventArgs.Empty);
        }
    }
}
