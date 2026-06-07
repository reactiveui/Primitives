// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Diagnostics.CodeAnalysis;
#if WINDOWS
using ReactiveUI.Primitives.ApiApproval;
using ReactiveUI.Primitives.Concurrency;
#endif

namespace ReactiveUI.Primitives.Wpf.Tests;

/// <summary>
/// Checks that the public API of ReactiveUI.Primitives.Wpf is consistent with previous releases,
/// highlighting any new or changed API. The real check runs only on Windows (where the WindowsDesktop
/// assembly can load); on other platforms it is a no-op so the cross-platform CI leg stays green.
/// </summary>
[ExcludeFromCodeCoverage]
public class ApiApprovalTests
{
    /// <summary>Generates the public API for the ReactiveUI.Primitives.Wpf assembly.</summary>
    /// <returns>A task representing the approval verification.</returns>
    [Test]
    public Task Wpf() =>
#if WINDOWS
        typeof(DispatcherSequencer).Assembly.CheckApproval();
#else
        Task.CompletedTask;
#endif
}
