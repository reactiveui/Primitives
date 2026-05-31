// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.ApiApproval;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>
/// Checks that the public API of ReactiveUI.Primitives.Extensions is consistent with previous releases,
/// highlighting any new or changed API.
/// </summary>
[ExcludeFromCodeCoverage]
public class ApiApprovalTests
{
    /// <summary>
    /// Generates the public API for the ReactiveUI.Primitives.Extensions assembly.
    /// </summary>
    /// <returns>A task to monitor the process.</returns>
    [Test]
    public Task Extensions() =>
        typeof(ReactiveExtensions).Assembly.CheckApproval();
}
