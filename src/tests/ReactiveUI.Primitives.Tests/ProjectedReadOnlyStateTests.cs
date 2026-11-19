// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ProjectedReadOnlyState{TSource, TResult}"/>.</summary>
public sealed class ProjectedReadOnlyStateTests
{
    /// <summary>The initial source value.</summary>
    private const int Initial = 1;

    /// <summary>Verifies an upstream error after creation releases the stored subscription and is rethrown by <c>Value</c>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_AfterCreate_StoresErrorForValue()
    {
        InvalidOperationException boom = new("projected");
        using StateSignal<int> upstream = new(Initial);
        using var projected = ProjectedReadOnlyState<int, int>.Create(upstream, static x => x);

        upstream.OnError(boom);

        var thrown = await Assert.That(() => projected.Value).ThrowsExactly<InvalidOperationException>();
        await Assert.That(thrown).IsSameReferenceAs(boom);
    }

    /// <summary>Verifies a selector that throws on the initial replay fails <c>Create</c> before any subscription is stored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_SelectorThrowsDuringCreate_CreateRethrows()
    {
        InvalidOperationException boom = new("selector");
        using StateSignal<int> upstream = new(Initial);

        var thrown = await Assert.That(() => ProjectedReadOnlyState<int, int>.Create(upstream, _ => throw boom))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(boom);
    }
}
