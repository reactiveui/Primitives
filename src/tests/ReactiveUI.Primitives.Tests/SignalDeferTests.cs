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
        deferred.Subscribe(values.Add);
        deferred.Subscribe(_ => { });
        await Assert.That(values.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(subscriptions).IsEqualTo(ExpectedSubscriptionCount);
        await Assert.That(Signal.FromEnumerable([First, Second]).ToEnumerable().SequenceEqual([First, Second]))
            .IsTrue();
        InvalidOperationException factoryError = new("defer-factory");
        Exception? observedFactoryError = null;
        Signal.Defer<int>(() => throw factoryError).Subscribe(_ => { }, ex => observedFactoryError = ex);
        await Assert.That(observedFactoryError!).IsSameReferenceAs(factoryError);
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Fail<int>(new InvalidOperationException("enumerable")).ToEnumerable());
        Assert.Throws<ArgumentNullException>(() => Signal.Defer<int>(null!));
        Assert.Throws<ArgumentNullException>(() => ((IObservable<int>)null!).ToEnumerable());
    }
}
