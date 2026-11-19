// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref="WhereIsNotNullObservable{T}"/>.</summary>
public class WhereIsNotNullObservableTests
{
    /// <summary>Verifies empty, never and already-filtered sources pass through and any other source gets a filter stage.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreatedOverConstantFilteredOrLiveSource_ThenOnlyLiveSourceIsWrapped()
    {
        using Subject<string?> live = new();
        var filtered = WhereIsNotNullObservable<string?>.Create(live);

        await Assert.That(WhereIsNotNullObservable<string?>.Create(ImmutableEmptySignal<string?>.Instance))
            .IsSameReferenceAs(ImmutableEmptySignal<string?>.Instance);
        await Assert.That(WhereIsNotNullObservable<string?>.Create(ImmutableNeverSignal<string?>.Instance))
            .IsSameReferenceAs(ImmutableNeverSignal<string?>.Instance);
        await Assert.That(filtered).IsTypeOf<WhereIsNotNullObservable<string?>>();
        await Assert.That(WhereIsNotNullObservable<string?>.Create(filtered)).IsSameReferenceAs(filtered);
    }
}
