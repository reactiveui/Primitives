// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;

/// <summary>A keyed sub-signal: the values of one group produced by <c>GroupBy</c> or <c>GroupByUntil</c>.</summary>
/// <typeparam name="TKey">The type of the key shared by every value in the group.</typeparam>
/// <typeparam name="T">The type of the values in the group.</typeparam>
/// <remarks>
/// A subscriber receives the values published after it subscribes. Subscribing to a group that has ended completes the
/// subscriber at once. The source subscription stays alive until the outer subscription and every group subscription
/// have been disposed.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("GroupedSignal: Key = {Key}")]
public sealed class GroupedSignal<TKey, T> : IObservable<T>
{
    /// <summary>The window that carries the values of this group.</summary>
    private readonly SliceWindow<T> _window;

    /// <summary>Initializes a new instance of the <see cref="GroupedSignal{TKey, T}"/> class.</summary>
    /// <param name="key">The key shared by every value in the group.</param>
    /// <param name="window">The window that carries the values of the group.</param>
    /// <exception cref="ArgumentNullException"><paramref name="window"/> is <see langword="null"/>.</exception>
    public GroupedSignal(TKey key, SliceWindow<T> window)
    {
        Key = key;
        _window = window ?? throw new ArgumentNullException(nameof(window));
    }

    /// <summary>Gets the key shared by every value in the group.</summary>
    public TKey Key { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) => _window.Subscribe(observer);
}
