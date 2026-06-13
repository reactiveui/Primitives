// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Covers the secondary-dispose-failure swallow branch and the scheduler-error-forwarding path of <c>UsingActionObservable&lt;T&gt;</c>.</summary>
public partial class UsingActionObservableTests
{
    /// <summary>Verifies that when the action throws AND the resource also throws on dispose,
    /// the primary action exception is forwarded and the dispose failure is swallowed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenActionAndDisposeBothThrow_ThenPrimaryActionErrorForwardedAndDisposeSwallowed()
    {
        HookDisposable resource = new(static () => throw new InvalidOperationException("dispose failed"));
        Exception? caught = null;
        InvalidOperationException actionFailure = new("action failed");

        using var sub = resource.Using(_ => throw actionFailure)
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(actionFailure);
        await Assert.That(resource.DisposeAttempts).IsEqualTo(1);
    }

    /// <summary>Verifies that an action exception forwarded via the scheduler path also disposes the resource.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerPathActionThrows_ThenForwardsErrorAndDisposes()
    {
        CountingDisposable resource = new();
        TaskCompletionSource<Exception> faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new("scheduler action failed");

        using var sub = resource.Using(_ => throw expected, TaskPoolSequencer.Default)
            .Subscribe(static _ => { }, ex => faulted.TrySetResult(ex));

        var caught = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposable that delegates the side-effect of <c>Dispose</c> to a caller-supplied
    /// <see cref="Action"/>. Used by tests that intentionally exercise the secondary-failure
    /// swallow branch of <c>UsingActionObservable</c> by passing a throwing hook.</summary>
    private sealed class HookDisposable : IDisposable
    {
        /// <summary>Per-dispose hook invoked from <see cref="Dispose"/>.</summary>
        private readonly Action _onDispose;

        /// <summary>Initializes a new instance of the <see cref="HookDisposable"/> class.</summary>
        /// <param name="onDispose">The hook invoked from <see cref="Dispose"/>.</param>
        public HookDisposable(Action onDispose) => _onDispose = onDispose;

        /// <summary>Gets the number of times <see cref="Dispose"/> was attempted.</summary>
        public int DisposeAttempts { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            DisposeAttempts++;
            _onDispose();
        }
    }

    /// <summary>Disposable that simply counts dispose invocations without throwing.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
