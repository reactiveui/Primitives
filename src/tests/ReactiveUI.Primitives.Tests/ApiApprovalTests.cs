// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ApiApproval;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Checks that the public API of the core ReactiveUI.Primitives packages is consistent with previous
/// releases, highlighting any new or changed API.
/// </summary>
[ExcludeFromCodeCoverage]
public class ApiApprovalTests
{
    /// <summary>
    /// Generates the public API for the ReactiveUI.Primitives assembly.
    /// </summary>
    /// <returns>A task to monitor the process.</returns>
    [Test]
    public Task Primitives() =>
        typeof(ISequencer).Assembly.CheckApproval();
}
