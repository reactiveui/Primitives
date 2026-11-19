// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref = "NotObservable"/> - boolean negation, terminal forwarding, and the null-observer subscribe guard.</summary>
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
        _ = Assert.Throws<ArgumentNullException>(() => observable.Subscribe(null!));
    }

    /// <summary>Verifies nested <see cref = "NotObservable"/> unwraps its inner source and indexable signals emit inline.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNestedNotOrEnumerableSignal_ThenUnwrapsAndEmitsExpected()
    {
        using ReactiveUI.Primitives.Signals.Signal<bool> subject = new();
        List<bool> nestedValues = [];
        using var nestedSub = new NotObservable(new NotObservable(subject)).Subscribe(nestedValues.Add);
        subject.OnNext(true);
        subject.OnNext(false);
        await Assert.That(nestedValues).IsCollectionEqualTo([true, false]);

        List<bool> listValues = [];
        var listCompleted = false;
        using var listSub = new NotObservable(ReactiveUI.Primitives.Signals.Signal.FromEnumerable([true, false]))
            .Subscribe(listValues.Add, () => listCompleted = true);
        await Assert.That(listValues).IsCollectionEqualTo([false, true]);
        await Assert.That(listCompleted).IsTrue();
    }

    /// <summary>Verifies empty and never sources pass through unchanged and any other source gets a negation stage.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreatedOverConstantOrLiveSource_ThenOnlyLiveSourceIsWrapped()
    {
        using Subject<bool> live = new();

        await Assert.That(NotObservable.Create(ReactiveUI.Primitives.Advanced.ImmutableEmptySignal<bool>.Instance))
            .IsSameReferenceAs(ReactiveUI.Primitives.Advanced.ImmutableEmptySignal<bool>.Instance);
        await Assert.That(NotObservable.Create(ReactiveUI.Primitives.Advanced.ImmutableNeverSignal<bool>.Instance))
            .IsSameReferenceAs(ReactiveUI.Primitives.Advanced.ImmutableNeverSignal<bool>.Instance);
        await Assert.That(NotObservable.Create(live)).IsTypeOf<NotObservable>();
    }
}
