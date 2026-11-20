// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Internal;
using ReactiveUI.Primitives.Signals;
using CoreDirectSequenceEmission = ReactiveUI.Primitives.Core.Internal.DirectSequenceEmission;
using CoreStateCallbackPair = ReactiveUI.Primitives.Core.Internal.StateCallbackPair<System.Collections.Generic.List<int>, System.Action<System.Collections.Generic.List<int>, int>>;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="DirectSequenceEmission"/> and its shared state records.</summary>
public sealed class DirectSequenceEmissionTests
{
    /// <summary>Second list element.</summary>
    private const int Two = 2;

    /// <summary>Range length.</summary>
    private const int Three = 3;

    /// <summary>Four-element range length.</summary>
    private const int Four = 4;

    /// <summary>Final fold total.</summary>
    private const int Six = 6;

    /// <summary>Sample multiplier value.</summary>
    private const int Ten = 10;

    /// <summary>First projected sum.</summary>
    private const int Eleven = 11;

    /// <summary>Second projected sum.</summary>
    private const int Twelve = 12;

    /// <summary>Expected reduced total.</summary>
    private const int Sixteen = 16;

    /// <summary>Minimum accepted projected value.</summary>
    private const int Twenty = 20;

    /// <summary>Third projected value.</summary>
    private const int Thirty = 30;

    /// <summary>Verifies <see cref="DirectSequenceEmission.RequiresCurrentThread{TSource}"/> reflects <see cref="IRequireCurrentThread{T}"/>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RequiresCurrentThread_ReflectsSourceContract()
    {
        await Assert.That(CoreDirectSequenceEmission.RequiresCurrentThread(Signal.Sequence(0, Four))).IsFalse();
        await Assert.That(CoreDirectSequenceEmission.RequiresCurrentThread(new CurrentThreadStub(true))).IsTrue();
        await Assert.That(DirectSequenceEmission.RequiresCurrentThread(new CurrentThreadStub(false))).IsFalse();
    }

    /// <summary>Verifies <see cref="DirectSequenceEmission.ValidateCallbacks{T}"/> guards null arguments and preserves delegates.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValidateCallbacks_ValidatesArguments()
    {
        Action<int> next = static _ => { };
        Action<Exception> err = static _ => { };
        Action done = static () => { };

        await Assert.That(() => DirectSequenceEmission.ValidateCallbacks<int>(null!, err, done)).Throws<ArgumentNullException>();
        await Assert.That(() => DirectSequenceEmission.ValidateCallbacks(next, null!, done)).Throws<ArgumentNullException>();
        await Assert.That(() => DirectSequenceEmission.ValidateCallbacks(next, err, null!)).Throws<ArgumentNullException>();

        var valid = DirectSequenceEmission.ValidateCallbacks(next, err, done);
        await Assert.That(valid.OnNext).IsSameReferenceAs(next);
        await Assert.That(valid.OnError).IsSameReferenceAs(err);
        await Assert.That(ReferenceEquals(valid.OnCompleted, done)).IsTrue();
    }

    /// <summary>Verifies direct filtered, projected, and tapped emission fast paths across ranges, lists, and live fallbacks.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FilteredProjectedAndTappedFastPaths_HandleRangeListFaultAndFallback()
    {
        RecordingWitness<int> rangeObserver = new();
        var handledRange = CoreDirectSequenceEmission.TrySubscribeFiltered(
            Signal.Sequence(1, Four),
            Two,
            static (v, mod) => v % mod == 0,
            static (v, mod) => v % mod == 0,
            rangeObserver);
        await Assert.That(handledRange).IsTrue();
        await Assert.That(rangeObserver.Values.SequenceEqual([Two, Four])).IsTrue();
        await Assert.That(rangeObserver.Completed).IsEqualTo(1);

        RecordingWitness<string> listObserver = new();
        var handledList = DirectSequenceEmission.TrySubscribeFiltered(
            Signal.FromEnumerable(["a", "bb", "ccc"]),
            Two,
            static (_, _) => false,
            static (v, minLen) => v.Length >= minLen,
            listObserver);
        await Assert.That(handledList).IsTrue();
        await Assert.That(listObserver.Values.SequenceEqual(["bb", "ccc"])).IsTrue();

        RecordingWitness<int> projectedRange = new();
        var okRange = CoreDirectSequenceEmission.TrySubscribeProjected(
            Signal.Sequence(1, Two),
            Ten,
            static (v, add) => v + add,
            static (v, add) => v + add,
            projectedRange);
        await Assert.That(okRange).IsTrue();
        await Assert.That(projectedRange.Values.SequenceEqual([Eleven, Twelve])).IsTrue();

        List<int> tappedSideEffect = [];
        RecordingWitness<int> tappedRange = new();
        CoreStateCallbackPair tap = new(tappedSideEffect, static (sink, item) => sink.Add(item * Ten));
        var okTap = CoreDirectSequenceEmission.TrySubscribeTapped(Signal.Sequence(1, Two), tap, tappedRange);
        await Assert.That(okTap).IsTrue();
        await Assert.That(tappedSideEffect.SequenceEqual([Ten, Twenty])).IsTrue();
        await Assert.That(tappedRange.Values.SequenceEqual([1, Two])).IsTrue();

        using Signal<int> live = new();
        RecordingWitness<int> liveObserver = new();
        var handledLive = DirectSequenceEmission.TrySubscribeFiltered(
            live,
            0,
            static (_, _) => true,
            static (_, _) => true,
            liveObserver);
        await Assert.That(handledLive).IsFalse();
    }

    /// <summary>Verifies <see cref="DirectSequenceEmission.TrySubscribeFilteredProjection{TSource, TContext, TResult}"/> and callback fast paths.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FilteredProjectionAndCallbackFastPaths_EmitExpectedItems()
    {
        StagePair<Func<int, int>, Func<int, bool>> stages = new(static x => x * Ten, static y => y >= Twenty);
        RecordingWitness<int> observer = new();

        var handled = DirectSequenceEmission.TrySubscribeFilteredProjection(
            Signal.Sequence(1, Three),
            stages,
            static (v, s) =>
            {
                var p = s.First(v);
                return new ProjectedOutcome<int>(s.Second(p), p);
            },
            static (v, s) =>
            {
                var p = s.First(v);
                return new ProjectedOutcome<int>(s.Second(p), p);
            },
            observer);

        await Assert.That(handled).IsTrue();
        await Assert.That(observer.Values.SequenceEqual([Twenty, Thirty])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);

        List<int> callbackItems = [];
        var callbackCompleted = 0;
        DirectObserverCallbacks<int> callbacks = new(callbackItems.Add, static _ => { }, () => callbackCompleted++);
        var callbackHandled = DirectSequenceEmission.TrySubscribeRangeFilteredProjectionCallbacks(
            Signal.Sequence(1, Three),
            stages,
            static (v, s) =>
            {
                var p = s.First(v);
                return new ProjectedOutcome<int>(s.Second(p), p);
            },
            callbacks);

        await Assert.That(callbackHandled).IsTrue();
        await Assert.That(callbackItems.SequenceEqual([Twenty, Thirty])).IsTrue();
        await Assert.That(callbackCompleted).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref="DirectSequenceEmission.TrySubscribeAccumulated{TSource, TAccumulate}"/> for both running fold and final reduce modes.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TrySubscribeAccumulated_SupportsFoldAndReduceModes()
    {
        RecordingWitness<int> foldObserver = new();
        var foldOk = DirectSequenceEmission.TrySubscribeAccumulated(
            Signal.Sequence(1, Three),
            0,
            static (acc, x) => acc + x,
            emitEachStep: true,
            foldObserver);
        await Assert.That(foldOk).IsTrue();
        await Assert.That(foldObserver.Values.SequenceEqual([1, Three, Six])).IsTrue();

        RecordingWitness<int> reduceObserver = new();
        var reduceOk = DirectSequenceEmission.TrySubscribeAccumulated(
            Signal.FromEnumerable([1, Two, Three]),
            Ten,
            static (acc, x) => acc + x,
            emitEachStep: false,
            reduceObserver);
        await Assert.That(reduceOk).IsTrue();
        await Assert.That(reduceObserver.Values.SequenceEqual([Sixteen])).IsTrue();

        RecordingWitness<int> rangeReduceObserver = new();
        var rangeReduceOk = DirectSequenceEmission.TrySubscribeAccumulated(
            Signal.Sequence(1, Three),
            0,
            static (acc, x) => acc + x,
            emitEachStep: false,
            rangeReduceObserver);
        await Assert.That(rangeReduceOk).IsTrue();
        await Assert.That(rangeReduceObserver.Values.SequenceEqual([Six])).IsTrue();
    }

    /// <summary>Verifies range callback fast paths emit filtered items and route non-fatal callback exceptions to <c>OnError</c>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FastPathCallbackFaults_CallbackOverloads_RouteNonFatalExceptionsToOnError()
    {
        InvalidOperationException boom = new("boom");
        List<int> rangeFilteredItems = [];
        var rangeFilteredDone = 0;
        DirectObserverCallbacks<int> okCallbacks = new(rangeFilteredItems.Add, static _ => { }, () => rangeFilteredDone++);
        var rangeFilteredOk = DirectSequenceEmission.TrySubscribeRangeFilteredCallbacks(
            Signal.Sequence(1, Four),
            Two,
            static (v, mod) => v % mod == 0,
            okCallbacks);
        await Assert.That(rangeFilteredOk).IsTrue();
        await Assert.That(rangeFilteredItems.SequenceEqual([Two, Four])).IsTrue();
        await Assert.That(rangeFilteredDone).IsEqualTo(1);

        List<Exception> callbackErrors = [];
        DirectObserverCallbacks<int> errCallbacks = new(static _ => { }, callbackErrors.Add, static () => { });

        _ = DirectSequenceEmission.TrySubscribeRangeFilteredCallbacks(
            Signal.Sequence(1, Two),
            boom,
            static (_, ex) => throw ex,
            errCallbacks);
        _ = DirectSequenceEmission.TrySubscribeRangeFilteredProjectionCallbacks(
            Signal.Sequence(1, Two),
            boom,
            static (_, ex) => throw ex,
            errCallbacks);
        _ = DirectSequenceEmission.TrySubscribeProjectedCallbacks(
            Signal.Sequence(1, Two),
            boom,
            static (_, ex) => throw ex,
            static (_, ex) => throw ex,
            errCallbacks);

        await Assert.That(callbackErrors.Count).IsEqualTo(Three);
    }

    /// <summary>Verifies range and list observer fast paths route non-fatal callback exceptions to <c>OnError</c>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FastPathCallbackFaults_ObserverOverloads_RouteNonFatalExceptionsToOnError()
    {
        InvalidOperationException boom = new("boom");
        RecordingWitness<int> intErrObserver = new();
        _ = DirectSequenceEmission.TrySubscribeFiltered(
            Signal.Sequence(1, Two),
            boom,
            static (_, ex) => throw ex,
            static (_, ex) => throw ex,
            intErrObserver);
        _ = DirectSequenceEmission.TrySubscribeProjected(
            Signal.Sequence(1, Two),
            boom,
            static (_, ex) => throw ex,
            static (_, ex) => throw ex,
            intErrObserver);
        _ = DirectSequenceEmission.TrySubscribeProjected(
            Signal.FromEnumerable(["a"]),
            boom,
            static (_, ex) => throw ex,
            static (_, ex) => throw ex,
            intErrObserver);
        _ = DirectSequenceEmission.TrySubscribeTapped(
            Signal.Sequence(1, Two),
            new StateCallbackPair<Exception, Action<Exception, int>>(boom, static (ex, _) => throw ex),
            intErrObserver);
        _ = DirectSequenceEmission.TrySubscribeFilteredProjection(
            Signal.Sequence(1, Two),
            boom,
            static (_, ex) => throw ex,
            static (_, ex) => throw ex,
            intErrObserver);
        _ = DirectSequenceEmission.TrySubscribeFilteredProjection(
            Signal.FromEnumerable(["a"]),
            boom,
            static (_, ex) => throw ex,
            static (_, ex) => throw ex,
            intErrObserver);

        RecordingWitness<string> strErrObserver = new();
        _ = DirectSequenceEmission.TrySubscribeTapped(
            Signal.FromEnumerable(["a"]),
            new StateCallbackPair<Exception, Action<Exception, string>>(boom, static (ex, _) => throw ex),
            strErrObserver);

        await Assert.That(intErrObserver.Errors.Count).IsEqualTo(Six);
        await Assert.That(strErrObserver.Errors.Count).IsEqualTo(1);
    }

    /// <summary>Verifies tapped completion callbacks across ranges, lists, live fallbacks, and callback faults.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TrySubscribeTappedWithCompletion_HandlesRangeListFallbackAndFaults()
    {
        List<int> tappedRangeItems = [];
        var rangeCompletedRuns = 0;
        RecordingWitness<int> rangeTapObserver = new();
        StateCallbackPair<List<int>, Action<List<int>, int>> intTap = new(
            tappedRangeItems,
            static (sink, item) => sink.Add(item));
        var rangeTappedOk = DirectSequenceEmission.TrySubscribeTapped(
            Signal.Sequence(1, Two),
            intTap,
            () => rangeCompletedRuns++,
            rangeTapObserver);
        await Assert.That(rangeTappedOk).IsTrue();
        await Assert.That(tappedRangeItems.SequenceEqual([1, Two])).IsTrue();
        await Assert.That(rangeCompletedRuns).IsEqualTo(1);
        await Assert.That(rangeTapObserver.Completed).IsEqualTo(1);

        List<string> tappedListItems = [];
        var listCompletedRuns = 0;
        RecordingWitness<string> listTapObserver = new();
        StateCallbackPair<List<string>, Action<List<string>, string>> strTap = new(
            tappedListItems,
            static (sink, item) => sink.Add(item));
        var listTappedOk = DirectSequenceEmission.TrySubscribeTapped(
            Signal.FromEnumerable(["x", "y"]),
            strTap,
            () => listCompletedRuns++,
            listTapObserver);
        await Assert.That(listTappedOk).IsTrue();
        await Assert.That(tappedListItems.SequenceEqual(["x", "y"])).IsTrue();
        await Assert.That(listCompletedRuns).IsEqualTo(1);
        await Assert.That(listTapObserver.Completed).IsEqualTo(1);

        using Signal<int> live = new();
        await Assert.That(DirectSequenceEmission.TrySubscribeTapped(live, intTap, static () => { }, rangeTapObserver)).IsFalse();

        InvalidOperationException completeBoom = new("tap-complete");
        RecordingWitness<int> rangeCompleteErr = new();
        _ = DirectSequenceEmission.TrySubscribeTapped(
            Signal.Sequence(1, Two),
            intTap,
            () => throw completeBoom,
            rangeCompleteErr);
        await Assert.That(rangeCompleteErr.Errors.SequenceEqual([completeBoom])).IsTrue();

        RecordingWitness<string> listCompleteErr = new();
        _ = DirectSequenceEmission.TrySubscribeTapped(
            Signal.FromEnumerable(["x"]),
            strTap,
            () => throw completeBoom,
            listCompleteErr);
        await Assert.That(listCompleteErr.Errors.SequenceEqual([completeBoom])).IsTrue();

        await Assert.That(() => DirectSequenceEmission.TrySubscribeTapped(
            Signal.Sequence(1, 1),
            intTap,
            static () => throw new BadImageFormatException(),
            rangeCompleteErr)).Throws<BadImageFormatException>();
    }

    /// <summary>Verifies accumulated fast paths route non-fatal callback exceptions to OnError and rethrow fatal ones.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TrySubscribeAccumulated_CallbackFaultsRouteToOnErrorAndRethrowFatal()
    {
        using Signal<int> live = new();
        RecordingWitness<int> rangeFoldErr = new();
        await Assert.That(DirectSequenceEmission.TrySubscribeAccumulated(live, 0, static (a, b) => a + b, emitEachStep: true, rangeFoldErr)).IsFalse();

        InvalidOperationException accBoom = new("acc-boom");
        _ = DirectSequenceEmission.TrySubscribeAccumulated(
            Signal.Sequence(1, Three),
            0,
            (_, item) => item == Two ? throw accBoom : item,
            emitEachStep: true,
            rangeFoldErr);
        await Assert.That(rangeFoldErr.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(rangeFoldErr.Errors.SequenceEqual([accBoom])).IsTrue();
        await Assert.That(rangeFoldErr.Completed).IsEqualTo(0);

        RecordingWitness<int> listReduceErr = new();
        _ = DirectSequenceEmission.TrySubscribeAccumulated(
            Signal.FromEnumerable([1, Two]),
            0,
            (_, item) => item == Two ? throw accBoom : item,
            emitEachStep: false,
            listReduceErr);
        await Assert.That(listReduceErr.Values).IsEmpty();
        await Assert.That(listReduceErr.Errors.SequenceEqual([accBoom])).IsTrue();
        await Assert.That(listReduceErr.Completed).IsEqualTo(0);

        await Assert.That(() => DirectSequenceEmission.TrySubscribeAccumulated(
            Signal.Sequence(1, 1),
            0,
            static (_, _) => throw new BadImageFormatException(),
            emitEachStep: true,
            rangeFoldErr)).Throws<BadImageFormatException>();
    }

    /// <summary>Stub observable implementing <see cref="IRequireCurrentThread{T}"/>.</summary>
    /// <param name="required">Whether current-thread subscription is required.</param>
    private sealed class CurrentThreadStub(bool required) : IRequireCurrentThread<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsRequiredSubscribeOnCurrentThread() => required;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(IObserver<int> observer) => EmptyDisposable.Instance;
    }
}
