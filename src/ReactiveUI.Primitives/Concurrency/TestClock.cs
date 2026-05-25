// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Test-facing alias for <see cref="VirtualClock"/>.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public sealed class TestClock : VirtualClock
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TestClock"/> class at the default clock value.
    /// </summary>
    public TestClock()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TestClock"/> class.
    /// </summary>
    /// <param name="initialClock">Initial virtual time.</param>
    public TestClock(DateTimeOffset initialClock)
        : base(initialClock)
    {
    }
}
