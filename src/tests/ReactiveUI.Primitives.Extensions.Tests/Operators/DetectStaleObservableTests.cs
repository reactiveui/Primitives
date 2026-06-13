// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Coverage for <c>DetectStaleObservable</c>'s subscription-teardown branch — when the source
/// terminates synchronously during subscribe, the sink is already done by the time the upstream handle
/// is attached, so the attach disposes it instead of recording it.</summary>
public class DetectStaleObservableTests
{
    /// <summary>Staleness window used by the tests.</summary>
    private const int WindowTicks = 100;

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Verifies that a source erroring synchronously during subscribe forwards the error and
    /// disposes the upstream handle through the attach-after-terminated branch.</summary>
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

    /// <summary>Observable that synchronously errors during <c>Subscribe</c> and exposes the subscription
    /// handle it returned so tests can assert it was disposed.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    /// <param name = "error">The exception to emit synchronously.</param>
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
