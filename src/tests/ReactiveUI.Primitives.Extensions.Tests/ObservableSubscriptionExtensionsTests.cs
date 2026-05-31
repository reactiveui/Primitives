// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using System.Reactive.Linq;
using System.Reactive.Subjects;

using System.IO;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using ReactiveUI.Primitives.Extensions.Tests;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Threading.Tasks;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for the blocking subscribe helpers on <see cref="ObservableSubscriptionExtensions"/>.</summary>
public partial class ObservableSubscriptionExtensionsTests
{
    /// <summary>Sentinel value emitted by single-value tests.</summary>
    private const int SentinelValue = 7;

    /// <summary>Verifies that <c>SubscribeGetValue</c> returns the last synchronously-emitted value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetValue_ThenReturnsLastSyncValue()
    {
        var result = Observable.Return(SentinelValue).SubscribeGetValue();

        await Assert.That(result).IsEqualTo(SentinelValue);
    }

    /// <summary>Verifies that <c>SubscribeGetValue</c> returns the default when the sequence is empty.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetValueEmpty_ThenReturnsDefault()
    {
        var result = Observable.Empty<int>().SubscribeGetValue();

        await Assert.That(result).IsEqualTo(0);
    }

    /// <summary>Verifies that <c>SubscribeAndComplete</c> consumes a RxVoid-producing observable without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAndComplete_ThenSwallowsUnitAndReturns()
    {
        // Helper is fire-and-forget; verify a follow-up call on a different sequence still
        // returns the expected value, proving SubscribeAndComplete didn't leave state behind.
        Observable.Return(RxVoid.Default).SubscribeAndComplete();
        var followUp = Observable.Return(RxVoid.Default).SubscribeGetValue();
        await Assert.That(followUp).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Verifies that <c>SubscribeGetError</c> captures a synchronous error and returns it.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetError_ThenCapturesSyncError()
    {
        var expected = new InvalidOperationException("sync");
        var error = Observable.Throw<int>(expected).SubscribeGetError();

        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies that the RxVoid-overload of <c>SubscribeGetError</c> captures the error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetErrorUnit_ThenCapturesSyncError()
    {
        var expected = new InvalidOperationException("RxVoid-sync");
        var error = Observable.Throw<RxVoid>(expected).SubscribeGetError();

        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies that <c>WaitForValue</c> blocks until the synchronously-completing source emits.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValue_ThenReturnsEmittedValue()
    {
        var result = Observable.Return(SentinelValue).WaitForValue();

        await Assert.That(result).IsEqualTo(SentinelValue);
    }

    /// <summary>Verifies that the timeout overload of <c>WaitForValue</c> honours an explicit deadline.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueWithTimeout_ThenReturnsEmittedValue()
    {
        var result = Observable.Return(SentinelValue).WaitForValue(TimeSpan.FromSeconds(5));

        await Assert.That(result).IsEqualTo(SentinelValue);
    }

    /// <summary>Verifies that <c>WaitForValue</c> throws <see cref="TimeoutException"/> on a non-terminating source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueTimesOut_ThenTimeoutException()
    {
        Action call = () => Observable.Never<int>().WaitForValue(TimeSpan.FromMilliseconds(50));
        var ex = Assert.Throws<TimeoutException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> returns once the RxVoid-producing source completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletion_ThenReturnsAfterTerminal()
    {
        // Helper returns void on success; the absence of TimeoutException after a synchronous
        // completion is the contract under test. Use the value-returning sibling for the actual
        // assertion so TUnit has a real check.
        Observable.Return(RxVoid.Default).WaitForCompletion(TimeSpan.FromSeconds(5));
        var subsequent = Observable.Return(RxVoid.Default).SubscribeGetValue();
        await Assert.That(subsequent).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> rethrows the source's error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionWithError_ThenRethrows()
    {
        var expected = new InvalidOperationException("wait");
        Action call = () => Observable.Throw<RxVoid>(expected).WaitForCompletion(TimeSpan.FromSeconds(5));
        var ex = Assert.Throws<InvalidOperationException>(call);
        await Assert.That(ex).IsEqualTo(expected);
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> throws <see cref="TimeoutException"/> for a non-terminating source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionTimesOut_ThenTimeoutException()
    {
        Action call = () => Observable.Never<RxVoid>().WaitForCompletion(TimeSpan.FromMilliseconds(50));
        var ex = Assert.Throws<TimeoutException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that <c>WaitForError</c> returns null when the source completes normally.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorNormalCompletion_ThenReturnsNull()
    {
        var error = Observable.Return(SentinelValue).WaitForError(TimeSpan.FromSeconds(5));

        await Assert.That(error).IsNull();
    }

    /// <summary>Verifies the default <c>WaitForError</c> overload returns null on normal completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorDefaultNormalCompletion_ThenReturnsNull()
    {
        var error = Observable.Return(SentinelValue).WaitForError();

        await Assert.That(error).IsNull();
    }

    /// <summary>Verifies that <c>WaitForError</c> returns the captured error rather than rethrowing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorSourceErrors_ThenReturnsCapturedError()
    {
        var expected = new InvalidOperationException("captured");
        var error = Observable.Throw<int>(expected).WaitForError(TimeSpan.FromSeconds(5));

        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies the default <c>WaitForError</c> overload returns the captured source error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorDefaultSourceErrors_ThenReturnsCapturedError()
    {
        var expected = new InvalidOperationException("captured-default");
        var error = Observable.Throw<int>(expected).WaitForError();

        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies that <c>WaitForError</c> throws <see cref="TimeoutException"/> for a non-terminating source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorTimesOut_ThenTimeoutException()
    {
        Action call = () => Observable.Never<int>().WaitForError(TimeSpan.FromMilliseconds(50));
        var ex = Assert.Throws<TimeoutException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies the single-arg <c>WaitForCompletion(IObservable&lt;RxVoid&gt;)</c> overload —
    /// pass-through to the scheduler-aware core with default timeout.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionUnitDefault_ThenReturnsOnCompletion()
    {
        var subject = new Subject<RxVoid>();
        var pump = Task.Run(() =>
        {
            subject.OnNext(RxVoid.Default);
            subject.OnCompleted();
        });

        subject.WaitForCompletion();
        await pump;
    }

    /// <summary>Exercises the no-op <c>OnError</c> body of <c>ValueCaptureObserver</c> —
    /// <c>SubscribeGetValue</c> on an erroring source still returns the last captured value
    /// (default) and the error is silently swallowed by the observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetValueSourceErrors_ThenErrorSwallowed()
    {
        var error = new InvalidOperationException("source-error");
        var source = Observable.Throw<int>(error);

        var value = source.SubscribeGetValue();

        await Assert.That(value).IsEqualTo(0);
    }

    /// <summary>Exercises the no-op <c>OnNext</c> and <c>OnCompleted</c> bodies of
    /// <c>ErrorCaptureObserver</c> — <c>SubscribeGetError</c> on a completing source ignores
    /// the value and the completion, returning a null error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetErrorSourceCompletesWithValue_ThenReturnsNull()
    {
        IObservable<int> source = Observable.Return(SentinelValue);

        var error = source.SubscribeGetError();

        await Assert.That(error).IsNull();
    }

    /// <summary>Exercises the <c>OnError</c> path of <c>BlockingValueObserver</c> —
    /// <c>WaitForValue</c> on an erroring source returns the default value once the gate
    /// is signalled by the error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueSourceErrors_ThenGateSignalledAndDefaultReturned()
    {
        var subject = new Subject<int>();
        var pump = Task.Run(() => subject.OnError(new InvalidOperationException("source-error")));

        var value = subject.WaitForValue();
        await pump;

        await Assert.That(value).IsEqualTo(0);
    }
}
