// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that never emits a value and never terminates.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class ImmutableNeverSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The shared instance; the signal carries no per-subscription state.</summary>
    public static readonly ImmutableNeverSignal<T> Instance = new();

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Accepts the observer without ever notifying it.</summary>
    /// <param name="observer">The observer, which receives nothing.</param>
    /// <returns>An empty disposable; there is no subscription to release.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        EmptyDisposable.Instance;
}
