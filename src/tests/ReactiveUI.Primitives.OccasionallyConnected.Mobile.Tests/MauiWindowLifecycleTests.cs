// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui;
using Microsoft.Maui.Controls;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Exercises the actual MAUI window's lifecycle event bridge.</summary>
public sealed class MauiWindowLifecycleTests
{
    /// <summary>Checks MAUI lifecycle dispatch reaches the adapter and disposal unsubscribes.</summary>
    /// <returns>The test completion.</returns>
    [Test]
    public async Task ActualWindowLifecycleEventsAreForwardedAndUnsubscribed()
    {
        var window = new Window();
        var platformWindow = (IWindow)window;
        const int ExpectedResumes = 2;
        var lifecycle = new MauiWindowLifecycle(window, initiallySuspended: true);
        var suspends = 0;
        var resumes = 0;
        lifecycle.Suspending += (_, _) => suspends++;
        lifecycle.Resuming += (_, _) => resumes++;
        platformWindow.Created();
        await Assert.That(lifecycle.IsSuspended).IsFalse();
        platformWindow.Stopped();
        await Assert.That(lifecycle.IsSuspended).IsTrue();
        platformWindow.Resumed();
        await Assert.That(lifecycle.IsSuspended).IsFalse();
        await Assert.That(suspends).IsEqualTo(1);
        await Assert.That(resumes).IsEqualTo(ExpectedResumes);
        lifecycle.Dispose();
        lifecycle.Dispose();
        platformWindow.Stopped();
        await Assert.That(suspends).IsEqualTo(1);
    }
}
