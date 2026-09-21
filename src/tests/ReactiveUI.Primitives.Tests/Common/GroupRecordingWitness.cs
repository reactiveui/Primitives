// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Records the order of every notification of a sequence of groups and of the groups themselves.</summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="T">The value type of each group.</typeparam>
/// <remarks>
/// Each group is named <c>gKey</c>. The log holds <c>open gKey</c>, <c>gKey:value</c>, <c>gKey:done</c>,
/// <c>gKey:error message</c>, <c>outer:done</c> and <c>outer:error message</c>. Groups subscribe on arrival unless
/// <see cref="SubscribeOnOpen"/> is cleared.
/// </remarks>
internal sealed class GroupRecordingWitness<TKey, T> : IObserver<GroupedSignal<TKey, T>>
{
    /// <summary>Gets or sets a value indicating whether each group is subscribed as it arrives.</summary>
    internal bool SubscribeOnOpen { get; set; } = true;

    /// <summary>Gets the recorded notifications in the order they arrived.</summary>
    internal List<string> Log { get; } = [];

    /// <summary>Gets the groups in the order they arrived.</summary>
    internal List<GroupedSignal<TKey, T>> Groups { get; } = [];

    /// <summary>Gets the subscription to each group that was subscribed on arrival.</summary>
    internal List<IDisposable> Subscriptions { get; } = [];

    /// <summary>Gets the log as one line.</summary>
    internal string Text => string.Join(' ', Log);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Log.Add("outer:done");

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Log.Add($"outer:error {error.Message}");

    /// <inheritdoc/>
    public void OnNext(GroupedSignal<TKey, T> value)
    {
        var name = $"g{Convert.ToString(value.Key, CultureInfo.InvariantCulture)}";
        Groups.Add(value);
        Log.Add($"open {name}");
        if (SubscribeOnOpen)
        {
            Subscriptions.Add(value.Subscribe(new NamedObserver(name, Log)));
        }
    }

    /// <summary>Writes one group's notifications into the shared log.</summary>
    /// <param name="name">The group name.</param>
    /// <param name="log">The shared log.</param>
    private sealed class NamedObserver(string name, List<string> log) : IObserver<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => log.Add($"{name}:done");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => log.Add($"{name}:error {error.Message}");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => log.Add($"{name}:{Convert.ToString(value, CultureInfo.InvariantCulture)}");
    }
}
