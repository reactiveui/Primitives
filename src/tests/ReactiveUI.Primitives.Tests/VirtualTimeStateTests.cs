// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests argument validation of the virtual-time scheduler state.</summary>
public sealed class VirtualTimeStateTests
{
    /// <summary>A null clock addition is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullAdd_ThrowsArgumentNull() =>
        await Assert.That(static () => new VirtualTimeState<long, long>(0, Comparer<long>.Default, null!, ToDateTimeOffset, ToRelative))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null clock conversion is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullToDateTimeOffset_ThrowsArgumentNull() =>
        await Assert.That(static () => new VirtualTimeState<long, long>(0, Comparer<long>.Default, Add, null!, ToRelative))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null relative conversion is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullToRelative_ThrowsArgumentNull() =>
        await Assert.That(static () => new VirtualTimeState<long, long>(0, Comparer<long>.Default, Add, ToDateTimeOffset, null!))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>Adds a relative offset to an absolute clock value.</summary>
    /// <param name="absolute">The absolute clock value.</param>
    /// <param name="relative">The relative offset.</param>
    /// <returns>The advanced clock value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long Add(long absolute, long relative) => absolute + relative;

    /// <summary>Converts a clock value to a date and time.</summary>
    /// <param name="absolute">The absolute clock value.</param>
    /// <returns>The equivalent date and time.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTimeOffset ToDateTimeOffset(long absolute) => new(absolute, TimeSpan.Zero);

    /// <summary>Converts a duration to a relative clock value.</summary>
    /// <param name="duration">The duration.</param>
    /// <returns>The equivalent relative clock value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ToRelative(TimeSpan duration) => duration.Ticks;
}
