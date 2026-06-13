// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Covers the function-overload Using factory (<c>UsingFuncObservable&lt;T,TResult&gt;</c>).
/// Targets the secondary-dispose-failure swallow branch and the happy-path resource-disposal,
/// matching the methodology used for <c>UsingActionObservable</c>.</summary>
public class UsingFuncObservableTests
{
    /// <summary>Sentinel result emitted by the happy-path test.</summary>
    private const int Sentinel = 42;

    /// <summary>Verifies the happy path — the function's result is emitted, completion fires,
    /// and the resource is disposed exactly once.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFunctionSucceeds_ThenEmitsResultAndDisposesResource()
    {
        CountingDisposable resource = new();
        List<int> results = [];
        var completed = false;

        using var sub = resource.Using(static _ => Sentinel)
            .Subscribe(results.Add, () => completed = true);

        await Assert.That(results).IsCollectionEqualTo([Sentinel]);
        await Assert.That(completed).IsTrue();
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies the secondary-dispose-failure swallow branch — when the function
    /// throws AND the resource also throws on Dispose, the primary function exception is
    /// forwarded and the dispose failure is silently swallowed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFunctionAndDisposeBothThrow_ThenPrimaryErrorForwardedAndDisposeSwallowed()
    {
        HookDisposable resource = new(static () => throw new InvalidOperationException("dispose failed"));
        Exception? caught = null;
        InvalidOperationException functionFailure = new("function failed");

        using var sub = resource.Using(new Func<HookDisposable, int>(_ => throw functionFailure))
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(functionFailure);
        await Assert.That(resource.DisposeAttempts).IsEqualTo(1);
    }

    /// <summary>Verifies that a function exception forwarded via the scheduler path also disposes the resource.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSchedulerPathFunctionThrows_ThenForwardsErrorAndDisposes()
    {
        CountingDisposable resource = new();
        TaskCompletionSource<Exception> faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new("scheduler function failed");

        using var sub = resource
            .Using(new Func<CountingDisposable, int>(_ => throw expected), TaskPoolSequencer.Default)
            .Subscribe(static _ => { }, ex => faulted.TrySetResult(ex));

        var caught = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(resource.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposable that delegates Dispose to a caller-supplied <see cref="Action"/>.</summary>
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

    /// <summary>Disposable that counts dispose invocations without throwing.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
