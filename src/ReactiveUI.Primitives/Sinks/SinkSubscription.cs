// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives;

/// <summary>
/// Shared single-upstream-subscription management for sink observers. Operating on a caller-owned
/// <see cref="IDisposable"/> field through a <see langword="ref"/> parameter lets each sink implement
/// <see cref="IObserver{T}"/> directly — with no shared base class, and therefore no virtual-dispatch
/// overhead on the hot notification path — while still sharing the assign-once / dispose-once teardown.
/// </summary>
internal static class SinkSubscription
{
    /// <summary>Sentinel stored once a sink is disposed so any late subscription is torn down immediately.</summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>Assigns the upstream subscription, disposing it immediately if the sink already holds one or has been disposed.</summary>
    /// <param name="subscription">The caller-owned subscription field.</param>
    /// <param name="value">The upstream subscription to assign.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Set(ref IDisposable? subscription, IDisposable value)
    {
        if (Interlocked.CompareExchange(ref subscription, value, null) is null)
        {
            return;
        }

        value.Dispose();
    }

    /// <summary>Releases the upstream subscription exactly once, latching a sentinel so later assignments self-dispose.</summary>
    /// <param name="subscription">The caller-owned subscription field.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Dispose(ref IDisposable? subscription)
    {
        var target = Interlocked.Exchange(ref subscription, DisposedSentinel);
        if (target is null || ReferenceEquals(target, DisposedSentinel))
        {
            return;
        }

        target.Dispose();
    }
}
