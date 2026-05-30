// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;

namespace ReactiveUI.Primitives.Async.Tests;

/// <content>
/// Using tests for combining operators.
/// </content>
public partial class CombiningOperatorTests
{
    /// <summary>Tests Using creates resource, emits values, and disposes resource on completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingHappyPath_ThenResourceIsDisposedAfterCompletion()
    {
        var trackingResource = new TrackingAsyncDisposable();

        var result = await SignalAsync.Using(
            _ => new ValueTask<TrackingAsyncDisposable>(trackingResource),
            static _ => SignalAsync.Return(99)).ToListAsync();

        await Assert.That(result).Count().IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(Sentinel99);
        await Assert.That(trackingResource.IsDisposed).IsTrue();
    }

    /// <summary>Tests Using disposes resource when observable factory throws.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingObservableFactoryThrows_ThenResourceIsDisposed()
    {
        var trackingResource = new TrackingAsyncDisposable();

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
        var trackingResource = new TrackingAsyncDisposable();

        var result = await SignalAsync.Using(
            _ => new ValueTask<TrackingAsyncDisposable>(trackingResource),
            static _ => SignalAsync.Range(1, 3)).ToListAsync();

        const int ResultIndexThird = 2;

        await Assert.That(result).Count().IsEqualTo(SampleValue3);
        await Assert.That(result[0]).IsEqualTo(1);
        await Assert.That(result[1]).IsEqualTo(SampleValue2);
        await Assert.That(result[ResultIndexThird]).IsEqualTo(SampleValue3);
        await Assert.That(trackingResource.IsDisposed).IsTrue();
    }
}
