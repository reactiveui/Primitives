// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the action-form <c>Using</c> operator backed by
/// <c>UsingActionObservable&lt;T&gt;</c> — happy path, null action, scheduler dispatch,
/// and action-throws-then-disposes paths.</summary>
public partial class UsingActionObservableTests
{
    /// <summary>Synthetic error message attached to action failures.</summary>
    private const string ActionFailedMessage = "action failed";

    /// <summary>Verifies that <c>Using</c> with a null action still emits, completes,
    /// and disposes the resource.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingNullAction_ThenEmitsUnitCompletesAndDisposes()
    {
        var resource = new TrackedDisposable();
        var completed = false;
        var emitted = 0;

        using var sub = resource.Using(action: null)
            .Subscribe(_ => emitted++, () => completed = true);

        await Assert.That(emitted).IsEqualTo(1);
        await Assert.That(completed).IsTrue();
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that <c>Using</c> invokes the action against the resource
    /// and disposes after completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingActionInvoked_ThenActionRunsThenResourceDisposed()
    {
        var resource = new TrackedDisposable();
        var actionRan = false;
        var completed = false;

        using var sub = resource.Using(r =>
        {
            actionRan = true;
            ThrowIfDisposed(r);
        }).Subscribe(static _ => { }, () => completed = true);

        await Assert.That(actionRan).IsTrue();
        await Assert.That(completed).IsTrue();
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that an action exception forwards the error and still disposes the resource.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingActionThrows_ThenForwardsErrorAndDisposes()
    {
        var resource = new TrackedDisposable();
        Exception? caught = null;
        var expected = new InvalidOperationException(ActionFailedMessage);

        using var sub = resource.Using(_ => throw expected)
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that the scheduler overload dispatches via the scheduler and still
    /// invokes the action then disposes the resource.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingWithScheduler_ThenRunsViaScheduler()
    {
        var resource = new TrackedDisposable();
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var actionRan = false;

        using var sub = resource.Using(_ => actionRan = true, scheduler: TaskPoolSequencer.Default)
            .Subscribe(static _ => { }, () => completed.TrySetResult(true));

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(actionRan).IsTrue();
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Throws when the resource was disposed before the action ran.</summary>
    /// <param name="resource">The resource to check.</param>
    private static void ThrowIfDisposed(TrackedDisposable resource)
    {
        if (resource.DisposeCount == 0)
        {
            return;
        }

        throw new InvalidOperationException("disposed before action ran");
    }

    /// <summary>Disposable that counts how many times it has been disposed.</summary>
    private sealed class TrackedDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
