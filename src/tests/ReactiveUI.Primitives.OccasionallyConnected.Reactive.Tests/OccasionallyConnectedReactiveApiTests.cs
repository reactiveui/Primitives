// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Concurrency;
using ReactiveUI.Primitives.OccasionallyConnected.Reactive;
using TUnit.Assertions;
using TUnit.Core;

namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive.Tests;

/// <summary>Verifies the System.Reactive-facing OccasionallyConnected surface.</summary>
public sealed class OccasionallyConnectedReactiveApiTests
{
    /// <summary>Verifies that the builder accepts a System.Reactive scheduler.</summary>
    /// <returns>Completes when the assertion finishes.</returns>
    [Test]
    public async Task Builder_accepts_a_System_Reactive_scheduler()
    {
        var builder = new OccasionallyConnectedBuilder();
        var configured = builder.UseSequencer(CurrentThreadScheduler.Instance);

        await Assert.That(configured).IsSameReferenceAs(builder);
    }

    /// <summary>Verifies that the reactive test project resolves System.Reactive.Unit.</summary>
    /// <returns>Completes when the assertion finishes.</returns>
    [Test]
    public async Task Reactive_package_references_System_Reactive_Unit() =>
        await Assert.That(default(Unit)).IsEqualTo(Unit.Default);
}
