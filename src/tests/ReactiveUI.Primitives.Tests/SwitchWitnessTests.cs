// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="SwitchWitness{T}"/>.</summary>
public sealed partial class SwitchWitnessTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>A duplicate completion from the current inner source is suppressed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessSuppressesDuplicateCompletionFromCurrentInner()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        inner.Observer!.OnCompleted();
        outer.OnCompleted();
        inner.Observer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies switching inner sources forwards only the latest source's values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessForwardsOnlyLatestInnerValues()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);
        first.Observer!.OnNext(One);

        outer.OnNext(second);

        first.Observer.OnNext(One);
        second.Observer!.OnNext(Two);

        await Assert.That(observer.Values.Count).IsEqualTo(Two);
        await Assert.That(observer.Values[0]).IsEqualTo(One);
        await Assert.That(observer.Values[1]).IsEqualTo(Two);
    }

    /// <summary>Verifies a stale inner completion does not complete the witness.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessIgnoresStaleInnerCompletion()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);
        var staleFirst = first.Observer!;
        outer.OnNext(second);
        outer.OnCompleted();

        staleFirst.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(0);

        second.Observer!.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies outer completion before the inner finishes defers completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessDefersCompletionUntilActiveInnerCompletes()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        outer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(0);

        inner.Observer!.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies completion when the outer finishes with no inner ever active.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessCompletesWhenOuterCompletesWithNoInner()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies an outer error is forwarded once and gates later notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessForwardsOuterErrorAndGatesAfterwards()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        InvalidOperationException error = new("outer");
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        outer.OnError(error);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);

        inner.Observer!.OnNext(One);
        inner.Observer.OnCompleted();
        outer.OnNext(inner);
        outer.OnCompleted();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).HasSingleItem();
    }

    /// <summary>Verifies an inner error is forwarded once and gates later notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessForwardsInnerErrorAndGatesAfterwards()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        InvalidOperationException error = new("inner");
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        inner.Observer!.OnError(error);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);

        inner.Observer.OnNext(One);
        outer.OnCompleted();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).HasSingleItem();
    }

    /// <summary>Verifies a stale inner error (wrong version) is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessDropsStaleInnerError()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);
        var staleFirst = first.Observer!;
        outer.OnNext(second);

        staleFirst.OnError(new InvalidOperationException("stale"));

        await Assert.That(observer.Errors).IsEmpty();

        second.Observer!.OnNext(Two);
        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Values[0]).IsEqualTo(Two);
    }

    /// <summary>Verifies a new source arriving after a terminal is ignored.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessIgnoresSourceAfterTerminal()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(One);

        outer.OnNext(inner);
        await Assert.That(inner.Observer).IsNull();
    }

    /// <summary>Verifies a terminal witness gates a later source switch and outer error while the outer source stays live.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessGatesLateNotificationsAfterInnerError()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> late = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);

        first.Observer!.OnError(new InvalidOperationException("boom"));
        await Assert.That(observer.Errors).HasSingleItem();

        outer.OnNext(late);
        outer.OnError(new InvalidOperationException("late"));

        await Assert.That(late.Observer).IsNull();
        await Assert.That(observer.Errors).HasSingleItem();
    }

    /// <summary>Verifies disposal mid-switch disposes the active inner subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessDisposesActiveInnerOnDispose()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        DisposalObservable<int> inner = new();
        var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        inner.Observer!.OnNext(One);
        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Values[0]).IsEqualTo(One);

        subscription.Dispose();

        await Assert.That(inner.Disposed).IsTrue();
    }

    /// <summary>An observable that captures its observer for manual notification.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class CapturingObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>An observable that captures its observer and records subscription disposal.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class DisposalObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Gets a value indicating whether the subscription was disposed.</summary>
        public bool Disposed { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return new ActionDisposable(() => Disposed = true);
        }
    }
}
