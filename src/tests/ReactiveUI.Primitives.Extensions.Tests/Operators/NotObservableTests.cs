// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref = "NotObservable"/> — boolean negation, terminal forwarding, and the null-observer subscribe guard.</summary>
public class NotObservableTests
{
    /// <summary>Verifies values are negated and completion is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNotSourceEmitsAndCompletes_ThenValuesNegatedAndCompletes()
    {
        Subject<bool> subject = new();
        List<bool> values = [];
        var completed = false;
        using var sub = subject.Not().Subscribe(values.Add, () => completed = true);
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnCompleted();
        await Assert.That(values).IsCollectionEqualTo([false, true]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies source errors are forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNotSourceErrors_ThenErrorForwarded()
    {
        Subject<bool> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new("boom");
        using var sub = subject.Not().Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies subscribing with a null observer throws.</summary>
    [Test]
    public void WhenNotObserverNull_ThenSubscribeThrows()
    {
        NotObservable observable = new(new Subject<bool>());
        Assert.Throws<ArgumentNullException>(() => observable.Subscribe(null!));
    }
}
