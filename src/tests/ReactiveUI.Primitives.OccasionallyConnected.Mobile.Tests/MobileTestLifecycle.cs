// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Provides deterministic mobile lifecycle hints.</summary>
internal sealed class MobileTestLifecycle : IMobileLifecycle
{
    /// <inheritdoc/>
    public event EventHandler? Suspending;

    /// <inheritdoc/>
    public event EventHandler? Resuming;

    /// <inheritdoc/>
    public bool IsSuspended { get; private set; }

    /// <summary>Publishes a lifecycle hint.</summary>
    /// <param name="suspended">Whether the host is suspended.</param>
    internal void SetSuspended(bool suspended)
    {
        IsSuspended = suspended;
        if (suspended)
        {
            Suspending?.Invoke(this, EventArgs.Empty);
        }
        else
        {
            Resuming?.Invoke(this, EventArgs.Empty);
        }
    }
}
