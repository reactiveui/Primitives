// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the scheduled queue's debugger representation.</summary>
public sealed class SequencerQueueTests
{
    /// <summary>The debugger can render an empty queue without inspecting any scheduled items.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task DebuggerDisplay_WhenEmpty_ThenShowsTheQueue()
    {
        SequencerQueue<int> queue = new();

        await Assert.That(ReadDebuggerDisplay(queue)).IsEqualTo(queue.ToString());
    }

#if NET9_0_OR_GREATER
    /// <summary>Reads the property evaluated by the debugger.</summary>
    /// <param name="queue">The scheduled queue.</param>
    /// <returns>The debugger representation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string ReadDebuggerDisplay(SequencerQueue<int> queue) => DebuggerAccessor<int>.Read(queue);

    /// <summary>Matches the target type's generic context required by .NET 9 and later.</summary>
    /// <typeparam name="T">The queue's absolute time type.</typeparam>
    private static class DebuggerAccessor<T>
        where T : IComparable<T>
    {
        /// <summary>Invokes the getter evaluated by the debugger.</summary>
        /// <param name="queue">The scheduled queue.</param>
        /// <returns>The debugger text.</returns>
        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
        internal static extern string Read(SequencerQueue<T> queue);
    }
#else
    /// <summary>Reads the property evaluated by the debugger.</summary>
    /// <param name="queue">The scheduled queue.</param>
    /// <returns>The debugger representation.</returns>
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
    private static extern string ReadDebuggerDisplay(SequencerQueue<int> queue);
#endif
}
