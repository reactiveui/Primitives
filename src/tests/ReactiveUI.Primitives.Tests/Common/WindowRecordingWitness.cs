// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Records the order of every notification of a sequence of windows and of the windows themselves.</summary>
/// <typeparam name="T">The value type of each window.</typeparam>
/// <remarks>
/// Each opened window is named <c>w0</c>, <c>w1</c>, and so on. The log holds <c>open wN</c>, <c>wN:value</c>,
/// <c>wN:done</c>, <c>wN:error message</c>, <c>outer:done</c> and <c>outer:error message</c>.
/// </remarks>
internal sealed class WindowRecordingWitness<T> : IObserver<IObservable<T>>
{
    /// <summary>The number of windows opened so far.</summary>
    private int _windows;

    /// <summary>Gets the recorded notifications in the order they arrived.</summary>
    internal List<string> Log { get; } = [];

    /// <summary>Gets the windows in the order they opened.</summary>
    internal List<IObservable<T>> Windows { get; } = [];

    /// <summary>Gets the subscription to each window, in the order the windows opened.</summary>
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
    public void OnNext(IObservable<T> value)
    {
        var name = $"w{_windows.ToString(CultureInfo.InvariantCulture)}";
        _windows++;
        Windows.Add(value);
        Log.Add($"open {name}");
        Subscriptions.Add(value.Subscribe(new NamedObserver(name, Log)));
    }

    /// <summary>Writes one window's notifications into the shared log.</summary>
    /// <param name="name">The window name.</param>
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
