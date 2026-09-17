// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests candidate matching when a user callback ends the subscription.</summary>
public class FirstMatchFromCandidatesObservableTests
{
    /// <summary>Disposal from the match predicate prevents delivery of the value being evaluated.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNext_PredicateDisposesSubscription_DoesNotDeliverMatch()
    {
        using Subject<int> source = new();
        MutableDisposable subscription = new();
        List<int> values = [];
        var completed = false;
        int[] candidates = [0];
        subscription.Disposable = candidates.FirstMatchFromCandidates(
            _ => source,
            static value => value,
            _ =>
            {
                subscription.Dispose();
                return true;
            },
            -1).Subscribe(values.Add, () => completed = true);

        source.OnNext(1);

        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsFalse();
        await Assert.That(source.HasObservers).IsFalse();
    }
}
