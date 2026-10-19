// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using Microsoft.Maui.Controls;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Connects actual MAUI window lifecycle events to mobile synchronization hints.</summary>
/// <remarks>Attach on the UI thread before the window is created. One instance describes one window, not all app windows.</remarks>
[DebuggerDisplay("Suspended = {IsSuspended}")]
public sealed class MauiWindowLifecycle : IMobileLifecycle, IDisposable
{
    /// <summary>The observed window.</summary>
    private readonly Window _window;

    /// <summary>The last suspension hint.</summary>
    private int _suspended;

    /// <summary>Whether subscriptions were removed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="MauiWindowLifecycle"/> class.</summary>
    /// <param name="window">The MAUI window whose OS-backed lifecycle is observed.</param>
    /// <param name="initiallySuspended">The state at attachment, when the window already exists.</param>
    /// <exception cref="ArgumentNullException"><paramref name="window"/> is null.</exception>
    public MauiWindowLifecycle(Window window, bool initiallySuspended)
    {
        ArgumentNullException.ThrowIfNull(window);
        _window = window;
        _suspended = initiallySuspended ? 1 : 0;
        window.Created += OnResuming;
        window.Resumed += OnResuming;
        window.Stopped += OnSuspending;
        window.Destroying += OnSuspending;
    }

    /// <inheritdoc/>
    public event EventHandler? Suspending;

    /// <inheritdoc/>
    public event EventHandler? Resuming;

    /// <inheritdoc/>
    public bool IsSuspended => Volatile.Read(ref _suspended) != 0;

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _window.Created -= OnResuming;
        _window.Resumed -= OnResuming;
        _window.Stopped -= OnSuspending;
        _window.Destroying -= OnSuspending;
    }

    /// <summary>Publishes a resume hint.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="args">The event arguments.</param>
    private void OnResuming(object? sender, EventArgs args)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        Volatile.Write(ref _suspended, 0);
        Resuming?.Invoke(this, EventArgs.Empty);
    }

    /// <summary>Publishes a suspend hint.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="args">The event arguments.</param>
    private void OnSuspending(object? sender, EventArgs args)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        Volatile.Write(ref _suspended, 1);
        Suspending?.Invoke(this, EventArgs.Empty);
    }
}
