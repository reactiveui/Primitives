// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for <see cref="CollaborationServerResourceScope"/> ownership semantics.</summary>
public sealed class CollaborationServerResourceScopeTests
{
    /// <summary>The endpoint resource name recorded by disposal tests.</summary>
    private const string EndpointResourceName = "endpoint";

    /// <summary>The hub resource name recorded by disposal tests.</summary>
    private const string HubResourceName = "hub";

    /// <summary>The endpoint disposal failure message.</summary>
    private const string EndpointFailureMessage = "endpoint failure";

    /// <summary>The hub disposal failure message.</summary>
    private const string HubFailureMessage = "hub failure";

    /// <summary>The original startup failure message used by rollback tests.</summary>
    private const string StartupFailureMessage = "startup failure";

    /// <summary>The expected count after both runtime resources have been disposed.</summary>
    private const int ExpectedPairResourceCount = 2;

    /// <summary>The expected count after a single runtime resource has been disposed.</summary>
    private const int ExpectedSingleResourceCount = 1;

    /// <summary>The timeout in seconds for blocked-resource tests.</summary>
    private const int WaitTimeoutSeconds = 5;

    /// <summary>Verifies runtime resources are disposed in reverse acquisition order.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncDisposesTrackedResourcesInReverseAcquisitionOrder()
    {
        var events = new List<string>();
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events));
        _ = scope.Track(new RecordingAsyncDisposable(EndpointResourceName, events));

        await scope.DisposeAsync().ConfigureAwait(false);

        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies disposal keeps releasing later resources after an earlier owner fails.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncAttemptsAllResourcesAndRethrowsFirstFailure()
    {
        var events = new List<string>();
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events, new InvalidOperationException(HubFailureMessage)));
        _ = scope.Track(new RecordingAsyncDisposable(EndpointResourceName, events, new InvalidDataException(EndpointFailureMessage)));

        var exception = await Assert.ThrowsExactlyAsync<InvalidDataException>(() => scope.DisposeAsync().AsTask());
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception?.Message).IsEqualTo(EndpointFailureMessage);
        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies concurrent disposal callers await the same blocked drain and observe the same first failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncSharesBlockedDrainAndFirstFailureWithConcurrentCaller()
    {
        var events = new List<string>();
        var blocker = new BlockingAsyncDisposable(EndpointResourceName, events, new InvalidDataException(EndpointFailureMessage));
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events));
        _ = scope.Track(blocker);

        var firstDispose = scope.DisposeAsync().AsTask();
        await blocker.Entered.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)).ConfigureAwait(false);
        var secondDispose = scope.DisposeAsync().AsTask();

        try
        {
            await Assert.That(ReferenceEquals(firstDispose, secondDispose)).IsTrue();
            await Assert.That(secondDispose.IsCompleted).IsFalse();
        }
        finally
        {
            blocker.Release();
        }

        var firstException = await Assert.ThrowsExactlyAsync<InvalidDataException>(() =>
            firstDispose.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)));
        var secondException = await Assert.ThrowsExactlyAsync<InvalidDataException>(() =>
            secondDispose.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)));
        await Assert.That(firstException).IsNotNull();
        await Assert.That(secondException).IsNotNull();
        await Assert.That(firstException?.Message).IsEqualTo(EndpointFailureMessage);
        await Assert.That(secondException?.Message).IsEqualTo(EndpointFailureMessage);
        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies resource callbacks observe the published shared task before cleanup starts.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncPublishesSharedTaskBeforeSynchronousResourceCallback()
    {
        var events = new List<string>();
        var scope = new CollaborationServerResourceScope();
        var reentrant = new ReentrantBlockingAsyncDisposable(EndpointResourceName, events, scope);
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events));
        _ = scope.Track(reentrant);

        var outerDispose = scope.DisposeAsync().AsTask();
        await reentrant.Entered.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)).ConfigureAwait(false);
        var reentrantDispose = reentrant.CapturedDisposeTask;

        try
        {
            await Assert.That(ReferenceEquals(outerDispose, reentrantDispose)).IsTrue();
            await Assert.That(reentrantDispose.IsCompleted).IsFalse();
        }
        finally
        {
            reentrant.Release();
        }

        await outerDispose.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)).ConfigureAwait(false);

        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies startup rollback releases every acquired owner without hiding the original startup error.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeSilentlyAsyncAttemptsAllResourcesWithoutSurfacingCleanupFailure()
    {
        var events = new List<string>();
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events, new InvalidOperationException(HubFailureMessage)));
        _ = scope.Track(new RecordingAsyncDisposable(EndpointResourceName, events, new InvalidDataException(EndpointFailureMessage)));

        await scope.DisposeSilentlyAsync().ConfigureAwait(false);

        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies silent startup rollback also completes normally when every acquired owner releases cleanly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeSilentlyAsyncReleasesSuccessfulResources()
    {
        var events = new List<string>();
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events));
        _ = scope.Track(new RecordingAsyncDisposable(EndpointResourceName, events));

        await scope.DisposeSilentlyAsync().ConfigureAwait(false);

        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies startup rollback waits for owned resources and preserves the startup exception.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RollbackAsyncWaitsForBlockedResourcesAndRethrowsStartupFailure()
    {
        var events = new List<string>();
        var blocker = new BlockingAsyncDisposable(EndpointResourceName, events);
        var startupFailure = new InvalidOperationException(StartupFailureMessage);
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events));
        _ = scope.Track(blocker);

        var rollback = scope.RollbackAsync<object>(startupFailure).AsTask();
        await blocker.Entered.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)).ConfigureAwait(false);

        try
        {
            await Assert.That(rollback.IsCompleted).IsFalse();
        }
        finally
        {
            blocker.Release();
        }

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            rollback.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)));
        await Assert.That(exception).IsSameReferenceAs(startupFailure);
        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies rollback waits for owned resources before reporting original startup cancellation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RollbackAsyncWaitsForBlockedResourcesBeforeReportingStartupCancellation()
    {
        var events = new List<string>();
        var blocker = new BlockingAsyncDisposable(EndpointResourceName, events);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var startupCancellation = new OperationCanceledException(cancellation.Token);
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events));
        _ = scope.Track(blocker);

        var rollback = scope.RollbackAsync<object>(startupCancellation).AsTask();
        await blocker.Entered.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)).ConfigureAwait(false);

        try
        {
            await Assert.That(rollback.IsCompleted).IsFalse();
        }
        finally
        {
            blocker.Release();
        }

        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() =>
            rollback.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds)));
        await Assert.That(rollback.IsCanceled).IsTrue();
        await Assert.That(exception?.CancellationToken).IsEqualTo(cancellation.Token);
        await AssertDisposedPairAsync(events).ConfigureAwait(false);
    }

    /// <summary>Verifies rollback rejects a missing original startup exception.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RollbackAsyncRejectsNullOriginalException()
    {
        var scope = new CollaborationServerResourceScope();

        await Assert.That(() => RollbackWithNullOriginal(scope)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies rollback without acquired resources faults with the original startup exception.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RollbackAsyncWithoutResourcesRethrowsStartupFailure()
    {
        var startupFailure = new InvalidOperationException(StartupFailureMessage);
        var scope = new CollaborationServerResourceScope();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            scope.RollbackAsync<object>(startupFailure).AsTask());

        await Assert.That(exception).IsSameReferenceAs(startupFailure);
    }

    /// <summary>Verifies rollback preserves the startup exception after observing a prior cleanup failure.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RollbackAsyncAfterFaultedCleanupRethrowsStartupFailure()
    {
        var events = new List<string>();
        var cleanupFailure = new InvalidDataException(EndpointFailureMessage);
        var startupFailure = new InvalidOperationException(StartupFailureMessage);
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(EndpointResourceName, events, cleanupFailure));

        _ = await Assert.ThrowsExactlyAsync<InvalidDataException>(() => scope.DisposeAsync().AsTask());
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            scope.RollbackAsync<object>(startupFailure).AsTask());

        await Assert.That(exception).IsSameReferenceAs(startupFailure);
        await Assert.That(events).Count().IsEqualTo(ExpectedSingleResourceCount);
    }

    /// <summary>Verifies disposing the scope twice does not repeat disposal side effects.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncIgnoresRepeatedDisposal()
    {
        var events = new List<string>();
        var scope = new CollaborationServerResourceScope();
        _ = scope.Track(new RecordingAsyncDisposable(HubResourceName, events));

        await scope.DisposeAsync().ConfigureAwait(false);
        await scope.DisposeAsync().ConfigureAwait(false);

        await Assert.That(events).Count().IsEqualTo(ExpectedSingleResourceCount);
        await Assert.That(events[0]).IsEqualTo(HubResourceName);
    }

    /// <summary>Verifies ownership cannot be transferred into a disposed scope.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TrackRejectsResourceAfterDisposal()
    {
        var events = new List<string>();
        var scope = new CollaborationServerResourceScope();

        await scope.DisposeAsync().ConfigureAwait(false);

        await Assert.That(() => scope.Track(new RecordingAsyncDisposable(HubResourceName, events))).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Asserts a pair of tracked resources were disposed in reverse acquisition order.</summary>
    /// <param name="events">The recorded disposal events.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertDisposedPairAsync(List<string> events)
    {
        await Assert.That(events).Count().IsEqualTo(ExpectedPairResourceCount);
        await Assert.That(events[0]).IsEqualTo(EndpointResourceName);
        await Assert.That(events[1]).IsEqualTo(HubResourceName);
    }

    /// <summary>Calls rollback with a null original exception from a runtime-created array slot.</summary>
    /// <param name="scope">The resource scope.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RollbackWithNullOriginal(CollaborationServerResourceScope scope)
    {
        var exceptions = new Exception[1];
        _ = scope.RollbackAsync<object>(exceptions[0]).AsTask();
    }

    /// <summary>Records disposal and optionally waits before failing.</summary>
    private sealed class BlockingAsyncDisposable : IAsyncDisposable
    {
        /// <summary>The optional disposal failure.</summary>
        private readonly Exception? _failure;

        /// <summary>The shared disposal event log.</summary>
        private readonly List<string> _events;

        /// <summary>The signal completed when disposal has started and is blocked.</summary>
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The signal released when disposal is allowed to complete.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The resource name written to the event log.</summary>
        private readonly string _name;

        /// <summary>Initializes a new instance of the <see cref="BlockingAsyncDisposable"/> class.</summary>
        /// <param name="name">The resource name.</param>
        /// <param name="events">The shared disposal event log.</param>
        /// <param name="failure">The optional disposal failure.</param>
        internal BlockingAsyncDisposable(string name, List<string> events, Exception? failure = null)
        {
            _name = name;
            _events = events;
            _failure = failure;
        }

        /// <summary>Gets the task completed when disposal has started and is blocked.</summary>
        internal Task Entered => _entered.Task;

        /// <summary>Allows the blocked disposal to complete.</summary>
        internal void Release() =>
            _ = _release.TrySetResult();

        /// <summary>Completes a disposal operation after an optional release signal.</summary>
        /// <param name="release">The release signal.</param>
        /// <param name="failure">The optional failure.</param>
        /// <returns>The asynchronous disposal task.</returns>
        private static async Task CompleteDisposalAsync(Task release, Exception? failure)
        {
            await release.ConfigureAwait(false);
            if (failure is null)
            {
                return;
            }

            ExceptionDispatchInfo.Throw(failure);
        }

        /// <inheritdoc/>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            _events.Add(_name);
            _ = _entered.TrySetResult();
            return new(CompleteDisposalAsync(_release.Task, _failure));
        }
    }

    /// <summary>Records disposal, reenters the owning scope synchronously and waits before completing.</summary>
    private sealed class ReentrantBlockingAsyncDisposable : IAsyncDisposable
    {
        /// <summary>The shared disposal event log.</summary>
        private readonly List<string> _events;

        /// <summary>The owning scope called again during disposal.</summary>
        private readonly CollaborationServerResourceScope _scope;

        /// <summary>The signal completed when disposal has started and is blocked.</summary>
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The signal released when disposal is allowed to complete.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The resource name written to the event log.</summary>
        private readonly string _name;

        /// <summary>The disposal task captured from the synchronous reentrant callback.</summary>
        private Task? _capturedDisposeTask;

        /// <summary>Initializes a new instance of the <see cref="ReentrantBlockingAsyncDisposable"/> class.</summary>
        /// <param name="name">The resource name.</param>
        /// <param name="events">The shared disposal event log.</param>
        /// <param name="scope">The owning scope.</param>
        internal ReentrantBlockingAsyncDisposable(string name, List<string> events, CollaborationServerResourceScope scope)
        {
            _name = name;
            _events = events;
            _scope = scope;
        }

        /// <summary>Gets the task completed when disposal has started and is blocked.</summary>
        internal Task Entered => _entered.Task;

        /// <summary>Gets the disposal task captured from the synchronous reentrant callback.</summary>
        internal Task CapturedDisposeTask => _capturedDisposeTask ?? throw new InvalidOperationException("Dispose has not reentered yet.");

        /// <summary>Allows the blocked disposal to complete.</summary>
        internal void Release() =>
            _ = _release.TrySetResult();

        /// <inheritdoc/>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            _events.Add(_name);
            _capturedDisposeTask = _scope.DisposeAsync().AsTask();
            _ = _entered.TrySetResult();
            return new(_release.Task);
        }
    }

    /// <summary>Records disposal and optionally fails while disposing.</summary>
    private sealed class RecordingAsyncDisposable : IAsyncDisposable
    {
        /// <summary>The optional disposal failure.</summary>
        private readonly Exception? _failure;

        /// <summary>The shared disposal event log.</summary>
        private readonly List<string> _events;

        /// <summary>The resource name written to the event log.</summary>
        private readonly string _name;

        /// <summary>Initializes a new instance of the <see cref="RecordingAsyncDisposable"/> class.</summary>
        /// <param name="name">The resource name.</param>
        /// <param name="events">The shared disposal event log.</param>
        /// <param name="failure">The optional disposal failure.</param>
        internal RecordingAsyncDisposable(string name, List<string> events, Exception? failure = null)
        {
            _name = name;
            _events = events;
            _failure = failure;
        }

        /// <inheritdoc/>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            _events.Add(_name);
            return _failure is null ? ValueTask.CompletedTask : ValueTask.FromException(_failure);
        }
    }
}
