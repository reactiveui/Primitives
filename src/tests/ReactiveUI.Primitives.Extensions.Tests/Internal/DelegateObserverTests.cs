// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for <see cref="DelegateObserver{T}"/> — verifies the null-callback branches
/// for <c>OnError</c> and <c>OnCompleted</c> as well as the all-callbacks-supplied happy path.</summary>
public class DelegateObserverTests
{
    /// <summary>Sentinel value emitted in single-value cases.</summary>
    private const int Sentinel = 42;

    /// <summary>Verifies that omitting the error callback still allows <c>OnError</c> to be invoked safely.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenErrorCallbackNull_ThenOnErrorIsNoOp()
    {
        var values = new List<int>();
        var observer = new DelegateObserver<int>(values.Add);

        observer.OnNext(1);
        observer.OnError(new InvalidOperationException("ignored"));

        await Assert.That(values).IsCollectionEqualTo([1]);
    }

    /// <summary>Verifies that omitting the completion callback still allows <c>OnCompleted</c> to be invoked safely.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCompletedCallbackNull_ThenOnCompletedIsNoOp()
    {
        var values = new List<int>();
        var observer = new DelegateObserver<int>(values.Add);

        observer.OnNext(Sentinel);
        observer.OnCompleted();

        await Assert.That(values).IsCollectionEqualTo([Sentinel]);
    }

    /// <summary>Verifies all three callbacks fire when supplied.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllCallbacksSupplied_ThenEachInvoked()
    {
        var values = new List<int>();
        Exception? caught = null;
        var completed = false;
        var observer = new DelegateObserver<int>(
            values.Add,
            ex => caught = ex,
            () => completed = true);

        observer.OnNext(1);
        observer.OnError(new InvalidOperationException("boom"));
        observer.OnCompleted();

        await Assert.That(values).IsCollectionEqualTo([1]);
        await Assert.That(caught).IsTypeOf<InvalidOperationException>();
        await Assert.That(completed).IsTrue();
    }
}
