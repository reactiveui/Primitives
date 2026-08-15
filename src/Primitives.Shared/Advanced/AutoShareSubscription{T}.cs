// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Reference-counted subscription handle.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Owner = {_owner}, Subscription = {Subscription}")]
public sealed class AutoShareSubscription<T> : IDisposable
{
    /// <summary>The owning gate; nulled once on dispose.</summary>
    private AutoShareSignal<T>? _owner;

    /// <summary>Initializes a new instance of the <see cref="AutoShareSubscription{T}"/> class.</summary>
    /// <param name="owner">The owning gate.</param>
    /// <param name="subscription">The inner source subscription.</param>
    public AutoShareSubscription(AutoShareSignal<T> owner, IDisposable subscription)
    {
        ArgumentExceptionHelper.ThrowIfNull(owner);
        ArgumentExceptionHelper.ThrowIfNull(subscription);

        _owner = owner;
        Subscription = subscription;
    }

    /// <summary>Gets the inner source subscription.</summary>
    private IDisposable Subscription { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Release(Subscription);
}
