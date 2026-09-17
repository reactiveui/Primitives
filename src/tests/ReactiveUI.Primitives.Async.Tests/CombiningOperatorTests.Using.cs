// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the Using operator.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Tests Using forwards a resumable source error to the subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingSourceErrorResumes_ThenErrorIsForwarded()
    {
        TrackingAsyncDisposable trackingResource = new();
        List<Exception> errors = [];
        Exception expected = new InvalidOperationException("resume");

        await using var subscription = await SignalAsync.Using(
                _ => new ValueTask<TrackingAsyncDisposable>(trackingResource),
                _ => SignalAsync.Create<int>(async (observer, ct) =>
                {
                    await observer.OnErrorResumeAsync(expected, ct);
                    await observer.OnCompletedAsync(Result.Success);
                    return DisposableAsync.Empty;
                }))
            .SubscribeAsync(
                static (_, _) => default,
                (error, _) =>
                {
                    errors.Add(error);
                    return default;
                });

        await Assert.That(errors).IsCollectionEqualTo([expected]);
        await Assert.That(trackingResource.IsDisposed).IsTrue();
    }

    /// <summary>Tests a faulted resource disposal still disposes the Using witness and surfaces the failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingResourceDisposeFaults_ThenWitnessDisposedAndFailureRethrown()
    {
        InvalidOperationException expected = new("resource dispose failed");
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        UsingWitness<FailingAsyncDisposable, int> witness = new(observer, new(expected));

        var error = await Assert.That(async () => await witness.DisposeAsync()).ThrowsExactly<InvalidOperationException>();

        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(witness.HasDisposed).IsTrue();
    }

    /// <summary>Tests Using disposes the resource through its witness when the source subscribe call throws.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingSourceSubscribeThrows_ThenResourceDisposedAndFailureRethrown()
    {
        TrackingAsyncDisposable trackingResource = new();
        InvalidOperationException expected = new("subscribe call failed");
        var observable = SignalAsync.Using<int, TrackingAsyncDisposable>(
            _ => new(trackingResource),
            _ => new ThrowingSubscribeSource<int>(expected));

        var error = await Assert.That(async () => await observable.SubscribeAsync(static (_, _) => default, null))
            .ThrowsExactly<InvalidOperationException>();

        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(trackingResource.IsDisposed).IsTrue();
    }

    /// <summary>Tests Using creates resource, emits values, and disposes resource on completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingHappyPath_ThenResourceIsDisposedAfterCompletion()
    {
        TrackingAsyncDisposable trackingResource = new();

        var result = await SignalAsync.Using(
            _ => new ValueTask<TrackingAsyncDisposable>(trackingResource),
            static _ => SignalAsync.Return(Sentinel99)).ToListAsync();

        await Assert.That(result).Count().IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(Sentinel99);
        await Assert.That(trackingResource.IsDisposed).IsTrue();
    }

    /// <summary>Tests Using disposes resource when observable factory throws.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingObservableFactoryThrows_ThenResourceIsDisposed()
    {
        TrackingAsyncDisposable trackingResource = new();

        var observable = SignalAsync.Using<int, TrackingAsyncDisposable>(
            _ => new(trackingResource),
            static _ => throw new InvalidOperationException("factory boom"));

        await Assert.That(async () => await observable.ToListAsync())
            .ThrowsException()
            .And.IsTypeOf<InvalidOperationException>();

        await Assert.That(trackingResource.IsDisposed).IsTrue();
    }

    /// <summary>Tests Using forwards cancellation token to resource factory.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingWithCancellation_ThenTokenIsForwardedToResourceFactory()
    {
        var factoryCalled = false;

        var observable = SignalAsync.Using<int, TrackingAsyncDisposable>(
            _ =>
            {
                factoryCalled = true;
                return new(new TrackingAsyncDisposable());
            },
            static _ => SignalAsync.Return(1));

        var result = await observable.ToListAsync();

        await Assert.That(factoryCalled).IsTrue();
        await Assert.That(result).Count().IsEqualTo(1);
    }

    /// <summary>Tests Using emits multiple values and still disposes resource.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingWithMultipleValues_ThenAllEmittedAndResourceDisposed()
    {
        TrackingAsyncDisposable trackingResource = new();

        const int SourceValueCount = 3;

        var result = await SignalAsync.Using(
            _ => new ValueTask<TrackingAsyncDisposable>(trackingResource),
            static _ => SignalAsync.Range(1, SourceValueCount)).ToListAsync();

        const int ResultIndexThird = 2;

        await Assert.That(result).Count().IsEqualTo(SampleValue3);
        await Assert.That(result[0]).IsEqualTo(1);
        await Assert.That(result[1]).IsEqualTo(SampleValue2);
        await Assert.That(result[ResultIndexThird]).IsEqualTo(SampleValue3);
        await Assert.That(trackingResource.IsDisposed).IsTrue();
    }
}
