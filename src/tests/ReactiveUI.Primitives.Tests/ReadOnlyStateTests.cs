// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests read-only state mirroring.</summary>
public class ReadOnlyStateTests
{
    /// <summary>The changed stream replays the initial state and forwards subsequent updates.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Changed_ReplaysInitialValueAndTracksSource()
    {
        const int UpdatedValue = 2;
        using Signal<int> source = new();
        using ReadOnlyState<int> state = new(source, 1);
        RecordingWitness<int> observer = new();
        using var subscription = state.Changed.Subscribe(observer);

        source.OnNext(UpdatedValue);

        await Assert.That(observer.Values.SequenceEqual([1, UpdatedValue])).IsTrue();
        await Assert.That(state.Value).IsEqualTo(UpdatedValue);
    }

    /// <summary>The debugger display leaves the cached value intact.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DebuggerDisplay_PreservesCachedValue()
    {
        using Signal<int> source = new();
        using ReadOnlyState<int> state = new(source, 1);
        await Assert.That(GetDebuggerDisplay(state)).IsEqualTo(state.ToString());
        await Assert.That(state.Value).IsEqualTo(1);
    }

    /// <summary>Invokes the getter used by the debugger without reflection.</summary>
    /// <param name="state">The instance to display.</param>
    /// <returns>The debugger display text.</returns>
#if NET9_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetDebuggerDisplay(ReadOnlyState<int> state) => DebuggerAccessor<int>.Read(state);

    /// <summary>Matches the target type's generic context required by .NET 9 and later.</summary>
    /// <typeparam name="T">The state's value type.</typeparam>
    private static class DebuggerAccessor<T>
    {
        /// <summary>Invokes the getter evaluated by the debugger.</summary>
        /// <param name="state">The state to display.</param>
        /// <returns>The debugger text.</returns>
        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
        internal static extern string Read(ReadOnlyState<T> state);
    }
#else
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
    private static extern string GetDebuggerDisplay(ReadOnlyState<int> state);
#endif
}
