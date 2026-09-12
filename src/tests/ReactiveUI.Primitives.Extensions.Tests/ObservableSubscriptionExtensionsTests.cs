// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for the blocking subscribe helpers on <see cref = "ObservableSubscriptionExtensions"/>.</summary>
public partial class ObservableSubscriptionExtensionsTests
{
    /// <summary>Sentinel value emitted by single-value tests.</summary>
    private const int SentinelValue = 7;

    /// <summary>Deadline handed to the blocking helpers when the source is expected to terminate.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Deadline handed to the blocking helpers when the source never terminates, so the wait must expire.</summary>
    private static readonly TimeSpan ExpiredTimeout = TimeSpan.Zero;

    /// <summary>Verifies that <c>SubscribeGetValue</c> returns the last synchronously-emitted value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetValue_ThenReturnsLastSyncValue()
    {
        var result = Observable.Return(SentinelValue).SubscribeGetValue();
        await Assert.That(result).IsEqualTo(SentinelValue);
    }

    /// <summary>Verifies that <c>SubscribeGetValue</c> returns the default when the sequence is empty.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetValueEmpty_ThenReturnsDefault()
    {
        var result = Observable.Empty<int>().SubscribeGetValue();
        await Assert.That(result).IsEqualTo(0);
    }

    /// <summary>Verifies that <c>SubscribeAndComplete</c> consumes a RxVoid-producing observable without error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAndComplete_ThenSwallowsUnitAndReturns()
    {
        // The helper returns nothing, so the follow-up call is what shows it left no state behind.
        Observable.Return(RxVoid.Default).SubscribeAndComplete();
        var followUp = Observable.Return(RxVoid.Default).SubscribeGetValue();
        await Assert.That(followUp).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Verifies that <c>SubscribeGetError</c> captures a synchronous error and returns it.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetError_ThenCapturesSyncError()
    {
        InvalidOperationException expected = new("sync");
        var error = Observable.Throw<int>(expected).SubscribeGetError();
        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies that the RxVoid-overload of <c>SubscribeGetError</c> captures the error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetErrorUnit_ThenCapturesSyncError()
    {
        InvalidOperationException expected = new("RxVoid-sync");
        var error = Observable.Throw<RxVoid>(expected).SubscribeGetError();
        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies that <c>WaitForValue</c> blocks until the synchronously-completing source emits.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValue_ThenReturnsEmittedValue()
    {
        var result = Observable.Return(SentinelValue).WaitForValue();
        await Assert.That(result).IsEqualTo(SentinelValue);
    }

    /// <summary>Verifies that the timeout overload of <c>WaitForValue</c> honours an explicit deadline.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueWithTimeout_ThenReturnsEmittedValue()
    {
        var result = Observable.Return(SentinelValue).WaitForValue(WaitTimeout);
        await Assert.That(result).IsEqualTo(SentinelValue);
    }

    /// <summary>Verifies that <c>WaitForValue</c> throws <see cref = "TimeoutException"/> on a non-terminating source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueTimesOut_ThenTimeoutException()
    {
        Action call = static () => Observable.Never<int>().WaitForValue(ExpiredTimeout);
        var ex = Assert.Throws<TimeoutException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> returns once the RxVoid-producing source completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletion_ThenReturnsAfterTerminal()
    {
        Observable.Return(RxVoid.Default).WaitForCompletion(WaitTimeout);
        var subsequent = Observable.Return(RxVoid.Default).SubscribeGetValue();
        await Assert.That(subsequent).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> rethrows the source's error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionWithError_ThenRethrows()
    {
        InvalidOperationException expected = new("wait");
        var call = () => Observable.Throw<RxVoid>(expected).WaitForCompletion(WaitTimeout);
        var ex = Assert.Throws<InvalidOperationException>(call);
        await Assert.That(ex).IsEqualTo(expected);
    }

    /// <summary>Verifies that <c>WaitForCompletion</c> throws <see cref = "TimeoutException"/> for a non-terminating source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionTimesOut_ThenTimeoutException()
    {
        var call = static () => Observable.Never<RxVoid>().WaitForCompletion(ExpiredTimeout);
        var ex = Assert.Throws<TimeoutException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that <c>WaitForError</c> returns null when the source completes normally.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorNormalCompletion_ThenReturnsNull()
    {
        var error = Observable.Return(SentinelValue).WaitForError(WaitTimeout);
        await Assert.That(error).IsNull();
    }

    /// <summary>Verifies the default <c>WaitForError</c> overload returns null on normal completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorDefaultNormalCompletion_ThenReturnsNull()
    {
        var error = Observable.Return(SentinelValue).WaitForError();
        await Assert.That(error).IsNull();
    }

    /// <summary>Verifies that <c>WaitForError</c> returns the captured error rather than rethrowing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorSourceErrors_ThenReturnsCapturedError()
    {
        InvalidOperationException expected = new("captured");
        var error = Observable.Throw<int>(expected).WaitForError(WaitTimeout);
        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies the default <c>WaitForError</c> overload returns the captured source error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorDefaultSourceErrors_ThenReturnsCapturedError()
    {
        InvalidOperationException expected = new("captured-default");
        var error = Observable.Throw<int>(expected).WaitForError();
        await Assert.That(error).IsEqualTo(expected);
    }

    /// <summary>Verifies that <c>WaitForError</c> throws <see cref = "TimeoutException"/> for a non-terminating source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForErrorTimesOut_ThenTimeoutException()
    {
        Action call = static () => Observable.Never<int>().WaitForError(ExpiredTimeout);
        var ex = Assert.Throws<TimeoutException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies the single-argument <c>WaitForCompletion</c> overload returns on completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForCompletionUnitDefault_ThenReturnsOnCompletion() =>
        await Assert.That(static () => Observable.Return(RxVoid.Default).WaitForCompletion()).ThrowsNothing();

    /// <summary>Verifies <c>SubscribeGetValue</c> swallows a source error and returns the default value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetValueSourceErrors_ThenErrorSwallowed()
    {
        InvalidOperationException error = new("source-error");
        var source = Observable.Throw<int>(error);
        var value = source.SubscribeGetValue();
        await Assert.That(value).IsEqualTo(0);
    }

    /// <summary>Verifies <c>SubscribeGetError</c> returns a null error for a source that completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeGetErrorSourceCompletesWithValue_ThenReturnsNull()
    {
        var source = Observable.Return(SentinelValue);
        var error = source.SubscribeGetError();
        await Assert.That(error).IsNull();
    }

    /// <summary>Verifies a source error releases <c>WaitForValue</c>, which returns the default value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitForValueSourceErrors_ThenGateSignalledAndDefaultReturned()
    {
        var value = Observable.Throw<int>(new InvalidOperationException("source-error")).WaitForValue();
        await Assert.That(value).IsEqualTo(0);
    }
}
