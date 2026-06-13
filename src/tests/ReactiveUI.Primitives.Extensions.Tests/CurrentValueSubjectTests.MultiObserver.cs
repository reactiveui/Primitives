// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Multi-observer and post-terminal coverage for <see cref = "CurrentValueSubject{T}"/>
/// — copy-on-write growth, mid-array unsubscribe, collapse back to single-observer, late
/// subscribers after error or completion, and dispose with active observers.</summary>
public partial class CurrentValueSubjectTests
{
    /// <summary>Initial value for multi-observer tests.</summary>
    private const int MultiInitialValue = 1;

    /// <summary>Verifies that three concurrent observers all receive the latest value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThreeObserversAndOnNext_ThenAllReceiveValue()
    {
        const int Update = 2;
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> a = [];
        List<int> b = [];
        List<int> c = [];
        using var subA = subject.Subscribe(a.Add);
        using var subB = subject.Subscribe(b.Add);
        using var subC = subject.Subscribe(c.Add);
        subject.OnNext(Update);
        await Assert.That(a).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(b).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(c).IsCollectionEqualTo([MultiInitialValue, Update]);
    }

    /// <summary>Verifies that disposing the middle observer of a 3-observer subject does not affect the other observers' delivery.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMiddleObserverDisposed_ThenOthersStillReceive()
    {
        const int Update = 2;
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> a = [];
        List<int> b = [];
        List<int> c = [];
        using var subA = subject.Subscribe(a.Add);
        var subB = subject.Subscribe(b.Add);
        using var subC = subject.Subscribe(c.Add);
        subB.Dispose();
        subject.OnNext(Update);
        await Assert.That(a).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(b).IsCollectionEqualTo([MultiInitialValue]);
        await Assert.That(c).IsCollectionEqualTo([MultiInitialValue, Update]);
    }

    /// <summary>Verifies that going from two observers back to one collapses to the
    /// single-observer fast path while still broadcasting correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSecondObserverDisposedFromPair_ThenSingleObserverStillReceives()
    {
        const int Update = 2;
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> a = [];
        List<int> b = [];
        using var subA = subject.Subscribe(a.Add);
        var subB = subject.Subscribe(b.Add);
        subB.Dispose();
        subject.OnNext(Update);
        await Assert.That(a).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(b).IsCollectionEqualTo([MultiInitialValue]);
    }

    /// <summary>Disposing the first observer of a 2-observer subject exercises Unsubscribe's
    /// <c>index == 0 ? existing[1] : existing[0]</c> ternary on the true branch — the surviving
    /// observer collapses back to the single-observer fast path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstObserverOfPairDisposed_ThenSingleSurvivorReceives()
    {
        const int Update = 2;
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> a = [];
        List<int> b = [];
        var subA = subject.Subscribe(a.Add);
        using var subB = subject.Subscribe(b.Add);

        // Dispose subA from the two-observer array; Unsubscribe's `index == 0 ? existing[1] : existing[0]`
        // ternary picks the true branch, collapsing _observer to subB.
        subA.Dispose();
        subject.OnNext(Update);
        await Assert.That(a).IsCollectionEqualTo([MultiInitialValue]);
        await Assert.That(b).IsCollectionEqualTo([MultiInitialValue, Update]);
    }

    /// <summary>Verifies that disposing the first observer of a 3-observer subject works
    /// (collapse exercises the index==0 branch of the shrink path).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstObserverDisposed_ThenOthersStillReceive()
    {
        const int Update = 2;
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> a = [];
        List<int> b = [];
        List<int> c = [];
        var subA = subject.Subscribe(a.Add);
        using var subB = subject.Subscribe(b.Add);
        using var subC = subject.Subscribe(c.Add);
        subA.Dispose();
        subject.OnNext(Update);
        await Assert.That(a).IsCollectionEqualTo([MultiInitialValue]);
        await Assert.That(b).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(c).IsCollectionEqualTo([MultiInitialValue, Update]);
    }

    /// <summary>Verifies that disposing the last observer of a 3-observer subject works
    /// (collapse exercises the tail-only branch of the shrink path).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastObserverDisposed_ThenOthersStillReceive()
    {
        const int Update = 2;
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> a = [];
        List<int> b = [];
        List<int> c = [];
        using var subA = subject.Subscribe(a.Add);
        using var subB = subject.Subscribe(b.Add);
        var subC = subject.Subscribe(c.Add);
        subC.Dispose();
        subject.OnNext(Update);
        await Assert.That(a).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(b).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(c).IsCollectionEqualTo([MultiInitialValue]);
    }

    /// <summary>Verifies that subscribing after the subject has errored immediately delivers the error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAfterError_ThenErrorDeliveredImmediately()
    {
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        InvalidOperationException expected = new("late-error");
        subject.OnError(expected);
        Exception? caught = null;
        using var sub = subject.Subscribe(
            static _ => { },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that subscribing after the subject has completed delivers the cached value and an immediate completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAfterCompleted_ThenReplayThenCompletes()
    {
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        subject.OnCompleted();
        List<int> values = [];
        var completed = false;
        using var sub = subject.Subscribe(values.Add, () => completed = true);
        await Assert.That(values).IsCollectionEqualTo([MultiInitialValue]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>OnError</c> broadcasts to multiple observers and is idempotent.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMultipleObserversAndOnError_ThenAllReceiveError()
    {
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        Exception? errA = null;
        Exception? errB = null;
        Exception? errC = null;
        InvalidOperationException expected = new("multi-error");
        using var subA = subject.Subscribe(
            static _ => { },
            ex => errA = ex);
        using var subB = subject.Subscribe(
            static _ => { },
            ex => errB = ex);
        using var subC = subject.Subscribe(
            static _ => { },
            ex => errC = ex);
        subject.OnError(expected);

        // Second OnError is a no-op.
        subject.OnError(new InvalidOperationException("ignored"));
        await Assert.That(errA).IsSameReferenceAs(expected);
        await Assert.That(errB).IsSameReferenceAs(expected);
        await Assert.That(errC).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>OnCompleted</c> broadcasts to multiple observers and a subsequent <c>OnCompleted</c> is a no-op.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMultipleObserversAndOnCompleted_ThenAllReceiveCompletionAndSecondIsNoOp()
    {
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        var completedA = 0;
        var completedB = 0;
        var completedC = 0;
        using var subA = subject.Subscribe(
            static _ => { },
            () => completedA++);
        using var subB = subject.Subscribe(
            static _ => { },
            () => completedB++);
        using var subC = subject.Subscribe(
            static _ => { },
            () => completedC++);
        subject.OnCompleted();
        subject.OnCompleted();
        await Assert.That(completedA).IsEqualTo(1);
        await Assert.That(completedB).IsEqualTo(1);
        await Assert.That(completedC).IsEqualTo(1);
    }

    /// <summary>Verifies that disposing the same subscription twice is idempotent.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscriptionDisposedTwice_ThenIdempotent()
    {
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> values = [];
        var sub = subject.Subscribe(values.Add);
        sub.Dispose();
        sub.Dispose();
        subject.OnNext(MultiInitialValue + 1);
        await Assert.That(values).IsCollectionEqualTo([MultiInitialValue]);
    }

    /// <summary>Verifies the multi-observer Unsubscribe path tolerates a stale dispose —
    /// after a middle observer is detached from a 4-observer array, disposing its returned
    /// subscription a second time hits the <c>Array.IndexOf</c> not-found early-return.
    /// The 4-observer setup keeps <c>_observers</c> non-null after the first dispose (the
    /// 2-observer setup collapses back to the single-observer fast path, which hits a
    /// different short-circuit instead of the IndexOf path).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMultiObserverDisposedTwice_ThenSecondDisposeIsNoOp()
    {
        const int Update = 2;
        using CurrentValueSubject<int> subject = new(MultiInitialValue);
        List<int> a = [];
        List<int> b = [];
        List<int> c = [];
        List<int> d = [];
        using var subA = subject.Subscribe(a.Add);
        var subB = subject.Subscribe(b.Add);
        using var subC = subject.Subscribe(c.Add);
        using var subD = subject.Subscribe(d.Add);
        subB.Dispose();
        subB.Dispose();
        subject.OnNext(Update);
        await Assert.That(a).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(b).IsCollectionEqualTo([MultiInitialValue]);
        await Assert.That(c).IsCollectionEqualTo([MultiInitialValue, Update]);
        await Assert.That(d).IsCollectionEqualTo([MultiInitialValue, Update]);
    }
}
