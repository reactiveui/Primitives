// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the terminal subscription marker.</summary>
public class DisposedMarkerTests
{
    /// <summary>Disposing a terminal marker is harmless even when repeated.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_Repeated_DoesNotThrow()
    {
        DisposedMarker marker = new();
        await Assert.That(() =>
        {
            marker.Dispose();
            marker.Dispose();
        }).ThrowsNothing();
    }
}
