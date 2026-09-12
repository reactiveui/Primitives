// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Assign-once, dispose-once management of a sink's single upstream subscription. Each helper takes the
/// caller-owned <see cref="IDisposable"/> field by <see langword="ref"/>, so a sink can implement
/// <see cref="IObserver{T}"/> directly without deriving from a shared base class.
/// </summary>
public static class SinkSubscription
{
    /// <summary>Sentinel stored once a sink is disposed so any late subscription is torn down immediately.</summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>Assigns the upstream subscription, disposing the incoming value when the field holds a subscription or the sink has been disposed.</summary>
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
