// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;
using CoreCancellationRaceHelper = ReactiveUI.Primitives.Core.Internal.CancellationRaceHelper;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="CancellationRaceHelper"/>.</summary>
public sealed class CancellationRaceHelperTests
{
    /// <summary>Verifies <see cref="CancellationRaceHelper.TryCancel"/> cancels active sources and swallows disposed sources.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryCancel_ActiveAndDisposedSources_ReturnsExpectedStatus()
    {
        using CancellationTokenSource active = new();
        await Assert.That(CancellationRaceHelper.TryCancel(active)).IsTrue();
        await Assert.That(active.IsCancellationRequested).IsTrue();

        CancellationTokenSource disposed = new();
        disposed.Dispose();
        await Assert.That(CancellationRaceHelper.TryCancel(disposed)).IsFalse();

        using CancellationTokenSource coreActive = new();
        await Assert.That(CoreCancellationRaceHelper.TryCancel(coreActive)).IsTrue();
        await Assert.That(coreActive.IsCancellationRequested).IsTrue();

        CancellationTokenSource coreDisposed = new();
        coreDisposed.Dispose();
        await Assert.That(CoreCancellationRaceHelper.TryCancel(coreDisposed)).IsFalse();
    }
}
