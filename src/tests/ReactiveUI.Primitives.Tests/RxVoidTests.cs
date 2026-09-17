// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the unit value's debugger representation.</summary>
public sealed class RxVoidTests
{
    /// <summary>The debugger renders the unit value using its normal textual representation.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task DebuggerDisplay_WhenDefault_ThenShowsUnit()
    {
        var value = RxVoid.Default;

        await Assert.That(ReadDebuggerDisplay(ref value)).IsEqualTo("()");
    }

    /// <summary>Reads the property evaluated by the debugger.</summary>
    /// <param name="value">The unit value.</param>
    /// <returns>The debugger representation.</returns>
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
    private static extern string ReadDebuggerDisplay(ref RxVoid value);
}
