// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Wpf.Tests;

/// <summary>
/// Placeholder so the cross-platform test host runs a test and does not report a zero-test run.
/// The real WPF dispatcher tests compile and run on Windows only (see the csproj).
/// </summary>
public sealed class CrossPlatformPlaceholderTests
{
    /// <summary>Verifies this build leg is the cross-platform one, where the WPF dispatcher tests are absent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatcherTestsRunOnWindowsOnly() =>
        await Assert.That(OperatingSystem.IsWindows()).IsFalse();
}
