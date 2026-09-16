// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests disposal of an upstream subscription returned after synchronous source termination.</summary>
public class DetectStaleObservableTests
{
    /// <summary>Staleness window used by the tests.</summary>
    private const int WindowTicks = 100;

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Verifies a source erroring during subscribe forwards the error and disposes the upstream handle.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceTerminatesDuringSubscribe_ThenLateAttachDisposesSubscription()
    {
        VirtualClock scheduler = new();
        InvalidOperationException expected = new(SourceErrorMessage);
        SyncErroringObservable<int> source = new(expected);
        Exception? caught = null;
        using var sub = source.DetectStale(TimeSpan.FromTicks(WindowTicks), scheduler).Subscribe(
            static _ => { },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(source.Subscription.IsDisposed).IsTrue();
    }

    /// <summary>Verifies an observer that marshals to another thread which completes the source does not deadlock the update delivery.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenObserverMarshalsCompletionDuringUpdate_ThenNoDeadlock() =>
        SerializedDeliveryAssertions.ObserverMarshallingCompletionDoesNotDeadlock<Stale<int>>(
            static (source, observer) => source.DetectStale(TimeSpan.FromTicks(WindowTicks), new VirtualClock()).Subscribe(observer),
            static observer => observer.OnNext(1));

    /// <summary>Observable that errors during <c>Subscribe</c> and exposes the handle it returned.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    /// <param name = "error">The exception to emit synchronously.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2315:A type that owns a disposable should be disposable",
        Justification = "The operator under test owns disposal of the exposed handle; this double only hands it back.")]
    private sealed class SyncErroringObservable<T>(Exception error) : IObservable<T>
    {
        /// <summary>Gets the subscription handle returned from the most recent subscribe.</summary>
        public BooleanDisposable Subscription { get; } = new();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnError(error);
            return Subscription;
        }
    }
}
