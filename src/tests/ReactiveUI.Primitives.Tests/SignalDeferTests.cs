// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> deferred-source and blocking enumeration contracts.</summary>
public sealed class SignalDeferTests
{
    /// <summary>Verifies deferred sources and blocking enumeration surface success, factory failure, and source failure paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DeferAndToEnumerableCoverSuccessAndErrorPaths()
    {
        const int First = 1;
        const int Second = 2;
        const int ExpectedSubscriptionCount = 2;
        var subscriptions = 0;
        List<int> values = [];
        var deferred = Signal.Defer(() =>
        {
            subscriptions++;
            return Signal.FromEnumerable([First, Second]);
        });
        _ = deferred.Subscribe(values.Add);
        _ = deferred.Subscribe(static _ => { });
        await Assert.That(values.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(subscriptions).IsEqualTo(ExpectedSubscriptionCount);
        await Assert.That(Signal.FromEnumerable([First, Second]).ToEnumerable().SequenceEqual([First, Second]))
            .IsTrue();
        InvalidOperationException factoryError = new("defer-factory");
        Exception? observedFactoryError = null;
        _ = Signal.Defer((Func<IObservable<int>>)(() => throw factoryError))
            .Subscribe(static _ => { }, ex => observedFactoryError = ex);
        await Assert.That(observedFactoryError!).IsSameReferenceAs(factoryError);
        _ = Assert.Throws<InvalidOperationException>(static () =>
            Signal.Fail<int>(new InvalidOperationException("enumerable")).ToEnumerable());
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Defer((Func<IObservable<int>>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).ToEnumerable());
    }
}
