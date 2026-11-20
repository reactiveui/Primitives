// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;
using CoreUnaryWitnessDelivery = ReactiveUI.Primitives.Core.Internal.UnaryWitnessDelivery;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="UnaryWitnessDelivery"/> and <see cref="UnaryWitnessSlot"/>.</summary>
public sealed class UnaryWitnessDeliveryTests
{
    /// <summary>Sample factor.</summary>
    private const int Two = 2;

    /// <summary>Sample input value.</summary>
    private const int Five = 5;

    /// <summary>Accepted value above threshold.</summary>
    private const int Seven = 7;

    /// <summary>Expected projected output.</summary>
    private const int Ten = 10;

    /// <summary>Verifies <see cref="UnaryWitnessSlot"/> forwards active items, enforces single terminal delivery, and disposes attached subscriptions.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task UnaryWitnessSlot_ForwardsActiveAndDisposesOnceStopped()
    {
        var upstreamDisposals = 0;
        var cleanupDisposals = 0;
        UnaryWitnessSlot slot = default;
        slot.Attach(new ActionDisposable(() => upstreamDisposals++));
        ActionDisposable cleanup = new(() =>
        {
            cleanupDisposals++;
            slot.Dispose();
        });
        RecordingWitness<int> observer = new();

        slot.NextFilteredProjection(
            Five,
            Two,
            static (v, mul) => new ProjectedOutcome<int>(true, v * mul),
            observer,
            cleanup);
        slot.NextFilteredProjection(
            Five,
            0,
            static (_, _) => default,
            observer,
            cleanup);
        slot.Complete(observer, cleanup);
        slot.NextProjected(Five, Two, static (v, m) => v * m, observer, cleanup);

        await Assert.That(observer.Values.SequenceEqual([Ten])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(upstreamDisposals).IsEqualTo(1);
        await Assert.That(cleanupDisposals).IsEqualTo(1);
    }

    /// <summary>Verifies attaching a subscription to an already-disposed <see cref="UnaryWitnessSlot"/> disposes it immediately.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task UnaryWitnessSlot_AttachAfterDispose_DisposesImmediately()
    {
        var disposed = 0;
        UnaryWitnessSlot slot = default;
        slot.Dispose();
        slot.Attach(new ActionDisposable(() => disposed++));

        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Verifies terminal transitions, post-stop suppression, and non-fatal callback fault routing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task UnaryWitnessDelivery_EnforcesSingleTerminalAndRoutesCallbackFaults()
    {
        var cleanupCount = 0;
        ActionDisposable cleanup = new(() => cleanupCount++);
        RecordingWitness<int> observer = new();
        var stopped = 0;

        CoreUnaryWitnessDelivery.NextFiltered(ref stopped, Seven, Five, static (v, min) => v > min, observer, cleanup);
        CoreUnaryWitnessDelivery.NextFiltered(ref stopped, Two, Five, static (v, min) => v > min, observer, cleanup);
        CoreUnaryWitnessDelivery.Complete(ref stopped, observer, cleanup);
        CoreUnaryWitnessDelivery.Complete(ref stopped, observer, cleanup);
        CoreUnaryWitnessDelivery.Fault(ref stopped, observer, new InvalidOperationException("late"), cleanup);

        await Assert.That(observer.Values.SequenceEqual([Seven])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(cleanupCount).IsEqualTo(1);

        var tapCleanupCount = 0;
        IDisposable? sub = new ActionDisposable(() => tapCleanupCount++);
        RecordingWitness<int> tapObserver = new();
        var tapStopped = 0;
        StateCallbackPair<int, Action<int, int>> tap = new(0, static (_, _) => throw new InvalidOperationException("tap"));

        UnaryWitnessDelivery.NextTapped(
            ref tapStopped,
            1,
            tap,
            tapObserver,
            new ActionDisposable(() => UnaryWitnessDelivery.Dispose(ref tapStopped, ref sub)));

        await Assert.That(tapObserver.Errors.Count).IsEqualTo(1);
        await Assert.That(tapCleanupCount).IsEqualTo(1);

        // Post-stop NextTapped is a no-op; active NextTapped forwards to observer.
        UnaryWitnessDelivery.NextTapped(ref tapStopped, 1, tap, tapObserver, EmptyDisposable.Instance);
        List<int> tappedValues = [];
        RecordingWitness<int> happyTapObserver = new();
        var activeTapStopped = 0;
        StateCallbackPair<List<int>, Action<List<int>, int>> happyTap = new(tappedValues, static (list, v) => list.Add(v));
        UnaryWitnessDelivery.NextTapped(ref activeTapStopped, Five, happyTap, happyTapObserver, EmptyDisposable.Instance);
        await Assert.That(tappedValues.SequenceEqual([Five])).IsTrue();
        await Assert.That(happyTapObserver.Values.SequenceEqual([Five])).IsTrue();

        // Post-stop and faulting NextFilteredProjection.
        var filteredStopped = 1;
        RecordingWitness<int> filteredObserver = new();
        UnaryWitnessDelivery.NextFilteredProjection(
            ref filteredStopped,
            1,
            0,
            static (_, _) => new ProjectedOutcome<int>(true, 1),
            filteredObserver,
            EmptyDisposable.Instance);
        await Assert.That(filteredObserver.Values).IsEmpty();

        var faultStopped = 0;
        var faultCleanup = 0;
        Func<int, int, ProjectedOutcome<int>> throwingProjection = static (_, _) =>
            throw new InvalidOperationException("fp-fault");
        UnaryWitnessDelivery.NextFilteredProjection(
            ref faultStopped,
            1,
            0,
            throwingProjection,
            filteredObserver,
            new ActionDisposable(() => faultCleanup++));
        await Assert.That(filteredObserver.Errors.Count).IsEqualTo(1);
        await Assert.That(faultCleanup).IsEqualTo(1);
    }

    /// <summary>Verifies value-bearing completion and tapped completion enforce stop state and route non-fatal callback faults.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompleteWithValueAndCompleteTapped_EnforceStopAndRouteFaults()
    {
        var valueStopped = 0;
        var valueCleanup = 0;
        ActionDisposable cleanup = new(() => valueCleanup++);
        RecordingWitness<int> valueObserver = new();

        UnaryWitnessDelivery.Complete(ref valueStopped, valueObserver, Ten, cleanup);
        UnaryWitnessDelivery.Complete(ref valueStopped, valueObserver, Five, cleanup);

        await Assert.That(valueObserver.Values.SequenceEqual([Ten])).IsTrue();
        await Assert.That(valueObserver.Completed).IsEqualTo(1);
        await Assert.That(valueCleanup).IsEqualTo(1);

        var tapDoneStopped = 0;
        var tapDoneRuns = 0;
        var tapDoneCleanup = 0;
        RecordingWitness<int> tapDoneObserver = new();
        UnaryWitnessDelivery.CompleteTapped(
            ref tapDoneStopped,
            () => tapDoneRuns++,
            tapDoneObserver,
            new ActionDisposable(() => tapDoneCleanup++));
        UnaryWitnessDelivery.CompleteTapped(
            ref tapDoneStopped,
            () => tapDoneRuns++,
            tapDoneObserver,
            EmptyDisposable.Instance);

        await Assert.That(tapDoneRuns).IsEqualTo(1);
        await Assert.That(tapDoneObserver.Completed).IsEqualTo(1);
        await Assert.That(tapDoneCleanup).IsEqualTo(1);

        InvalidOperationException completeFault = new("complete-fault");
        var tapCompleteFaultStopped = 0;
        RecordingWitness<int> tapCompleteFaultObserver = new();
        UnaryWitnessDelivery.CompleteTapped(
            ref tapCompleteFaultStopped,
            () => throw completeFault,
            tapCompleteFaultObserver,
            EmptyDisposable.Instance);
        await Assert.That(tapCompleteFaultObserver.Errors.SequenceEqual([completeFault])).IsTrue();
    }

    /// <summary>Verifies tapped fault handling and fatal-exception passthrough across all delivery helpers.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FaultTappedAndFatalPassthrough_EnforceStopAndRethrowFatal()
    {
        InvalidOperationException upstreamError = new("upstream-error");
        Exception? tappedSeen = null;
        var tapErrStopped = 0;
        RecordingWitness<int> tapErrObserver = new();
        UnaryWitnessDelivery.FaultTapped(
            ref tapErrStopped,
            upstreamError,
            ex => tappedSeen = ex,
            tapErrObserver,
            EmptyDisposable.Instance);
        UnaryWitnessDelivery.FaultTapped(
            ref tapErrStopped,
            upstreamError,
            static _ => throw new InvalidOperationException("ignored"),
            tapErrObserver,
            EmptyDisposable.Instance);

        await Assert.That(tappedSeen).IsSameReferenceAs(upstreamError);
        await Assert.That(tapErrObserver.Errors.SequenceEqual([upstreamError])).IsTrue();

        InvalidOperationException callbackError = new("callback-error");
        var tapErrThrowStopped = 0;
        RecordingWitness<int> tapErrThrowObserver = new();
        UnaryWitnessDelivery.FaultTapped(
            ref tapErrThrowStopped,
            upstreamError,
            _ => throw callbackError,
            tapErrThrowObserver,
            EmptyDisposable.Instance);
        await Assert.That(tapErrThrowObserver.Errors.SequenceEqual([callbackError])).IsTrue();

        await AssertTerminalFatalPassthrough(tapErrObserver);
        await AssertNextFatalPassthrough(tapErrObserver);
    }

    /// <summary>Verifies fatal exceptions bypass terminal <see cref="UnaryWitnessDelivery"/> exception filters.</summary>
    /// <param name="observer">The target observer.</param>
    /// <returns>A task representing the asynchronous assertions.</returns>
    private static async Task AssertTerminalFatalPassthrough(RecordingWitness<int> observer)
    {
        await Assert.That(() =>
        {
            var stopped = 0;
            UnaryWitnessDelivery.CompleteTapped(
                ref stopped,
                static () => throw new BadImageFormatException(),
                observer,
                EmptyDisposable.Instance);
        }).Throws<BadImageFormatException>();

        await Assert.That(() =>
        {
            var stopped = 0;
            UnaryWitnessDelivery.FaultTapped(
                ref stopped,
                new InvalidOperationException("source"),
                static _ => throw new BadImageFormatException(),
                observer,
                EmptyDisposable.Instance);
        }).Throws<BadImageFormatException>();
    }

    /// <summary>Verifies fatal exceptions bypass value <see cref="UnaryWitnessDelivery"/> exception filters.</summary>
    /// <param name="observer">The target observer.</param>
    /// <returns>A task representing the asynchronous assertions.</returns>
    private static async Task AssertNextFatalPassthrough(RecordingWitness<int> observer)
    {
        await Assert.That(() =>
        {
            var stopped = 0;
            CoreUnaryWitnessDelivery.NextFiltered(
                ref stopped,
                1,
                0,
                static (_, _) => throw new BadImageFormatException(),
                observer,
                EmptyDisposable.Instance);
        }).Throws<BadImageFormatException>();

        await Assert.That(() =>
        {
            var stopped = 0;
            UnaryWitnessDelivery.NextProjected(
                ref stopped,
                1,
                0,
                static (_, _) => throw new BadImageFormatException(),
                observer,
                EmptyDisposable.Instance);
        }).Throws<BadImageFormatException>();

        await Assert.That(() =>
        {
            var stopped = 0;
            UnaryWitnessDelivery.NextTapped(
                ref stopped,
                1,
                new StateCallbackPair<int, Action<int, int>>(0, static (_, _) => throw new BadImageFormatException()),
                observer,
                EmptyDisposable.Instance);
        }).Throws<BadImageFormatException>();

        await Assert.That(() =>
        {
            var stopped = 0;
            UnaryWitnessDelivery.NextFilteredProjection(
                ref stopped,
                1,
                0,
                static (_, _) => throw new BadImageFormatException(),
                observer,
                EmptyDisposable.Instance);
        }).Throws<BadImageFormatException>();
    }
}
