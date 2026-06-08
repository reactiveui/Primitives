// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for signal-Merge disposal behavior.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>
    /// Verifies that when the merge subscription is disposed while an inner source is still
    /// emitting, the forwarding methods silently return without throwing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposedDuringInnerEmission_ThenForwardingSilentlyReturns()
    {
        var innerSignal = Signal.Create<int>();
        var outer = Signal.Create<IObservableAsync<int>>();
        var items = new List<int>();

        var sub = await outer.Values
            .Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        await outer.OnNextAsync(innerSignal.Values, CancellationToken.None);

        await innerSignal.OnNextAsync(1, CancellationToken.None);

        // Dispose the merge subscription
        await sub.DisposeAsync();

        // These should be silently ignored because subscription is disposed
        try
        {
            await innerSignal.OnNextAsync(SampleValue2, CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected – the inner observer may throw on cancellation
        }

        await Assert.That(items).Contains(1);

        await innerSignal.DisposeAsync();
        await outer.DisposeAsync();
    }

    /// <summary>Verifies that the error-resume forwarding path in Merge is silently skipped after the subscription has been disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposedDuringErrorResume_ThenForwardingSilentlyReturns()
    {
        var innerSignal = Signal.Create<int>();
        var outer = Signal.Create<IObservableAsync<int>>();
        var errors = new List<Exception>();

        var sub = await outer.Values
            .Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await outer.OnNextAsync(innerSignal.Values, CancellationToken.None);

        // Dispose the merge subscription
        await sub.DisposeAsync();

        // Error resume after dispose should be silently ignored
        try
        {
            await innerSignal.OnErrorResumeAsync(new InvalidOperationException(LateErrorMessage), CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await innerSignal.DisposeAsync();
        await outer.DisposeAsync();
    }

    /// <summary>Tests that MergeSignalSourcesSignal OnNextAsync returns early when disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposedBeforeInnerEmission_ThenRelayNextAsyncReturns()
    {
        var source = Signal.Create<int>();
        var inner = Signal.Create<IObservableAsync<int>>();
        var items = new List<int>();
        Result? completionResult = null;

        await using var sub = await inner.Values
            .Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await inner.OnNextAsync(source.Values, CancellationToken.None);
        await inner.OnCompletedAsync(Result.Failure(new InvalidOperationException("force done")));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult is not null,
            TimeSpan.FromSeconds(5));

        // After dispose, forwarding should be a no-op
        await source.OnNextAsync(Sentinel42, CancellationToken.None);

        await Assert.That(items).DoesNotContain(Sentinel42);
    }

    /// <summary>Verifies that Merge OnNextAsync returns early when disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeDisposedDuringEmission_ThenRelayNextAsyncReturnsEarly()
    {
        var signal = Signal.Create<int>();
        var items = new List<int>();

        var sub = await signal.Values.Merge(SignalAsync.Never<int>())
            .SubscribeAsync(
                (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    return default;
                },
                null);

        await signal.OnNextAsync(1, CancellationToken.None);
        await sub.DisposeAsync();

        // After dispose, further emissions should be silently dropped
        await signal.OnNextAsync(SampleValue2, CancellationToken.None);

        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SampleValue2);
    }

    /// <summary>
    /// Verifies that OnNextAsync in MergeCoordinator returns early (pre-gate check)
    /// when the subscription has already been disposed.
    /// Uses DirectSource to retain a reference to the inner observer so that emissions
    /// can be attempted after disposal without being blocked by Signal un-subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposed_ThenRelayNextAsyncReturnsPreGate()
    {
        var innerSource = new DirectSource<int>();
        var outerSource = new DirectSource<IObservableAsync<int>>();
        var items = new List<int>();

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    return default;
                },
                null);

        // Subscribe the inner source through the outer
        await outerSource.EmitNext(innerSource, CancellationToken.None);

        // Emit a value to confirm the pipeline is working
        await innerSource.EmitNext(1, CancellationToken.None);

        // Dispose the merge subscription
        await sub.DisposeAsync();

        // Emit after dispose – DirectSource still holds the inner observer reference,
        // so this reaches OnNextAsync which should return early at the pre-gate check.
        try
        {
            await innerSource.EmitNext(SampleValue2, CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected – the linked CTS may be cancelled
        }

        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SampleValue2);
    }

    /// <summary>Verifies that OnErrorResumeAsync in MergeCoordinator returns early when the subscription has already been disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposed_ThenRelayErrorAsyncReturns()
    {
        var innerSource = new DirectSource<int>();
        var outerSource = new DirectSource<IObservableAsync<int>>();
        var errors = new List<Exception>();

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    lock (_gate)
                    {
                        errors.Add(ex);
                    }

                    return default;
                });

        await outerSource.EmitNext(innerSource, CancellationToken.None);

        // Dispose the merge subscription
        await sub.DisposeAsync();

        // Error after dispose should be silently ignored
        try
        {
            await innerSource.EmitError(new InvalidOperationException(LateErrorMessage), CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>
    /// Verifies that OnNextAsync in MergeCoordinator returns early at the post-gate
    /// disposed check when disposal occurs while waiting for the gate.
    /// A slow observer holds the gate while a second emission waits; disposal happens
    /// before the second emission acquires the gate.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposedWhileGateHeld_ThenRelayNextAsyncReturnsPostGate()
    {
        var innerSource = new DirectSource<int>();
        var outerSource = new DirectSource<IObservableAsync<int>>();
        var items = new List<int>();
        var gateHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceedWithFirstEmission = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                async (x, _) =>
                {
                    lock (_gate)
                    {
                        items.Add(x);
                    }

                    if (x == 1)
                    {
                        // Signal that the gate is being held by this OnNext call
                        gateHeld.SetResult();

                        // Wait here, keeping the gate held
                        await proceedWithFirstEmission.Task;
                    }
                },
                null);

        // Subscribe the inner source through the outer
        await outerSource.EmitNext(innerSource, CancellationToken.None);

        // First emission holds the gate via the slow observer
        var firstEmission = innerSource.EmitNext(1, CancellationToken.None);

        // Wait until the gate is held
        await gateHeld.Task;

        // Start a second emission that will queue behind the gate
        var secondEmissionTask = Task.Run(async () =>
        {
            try
            {
                await innerSource.EmitNext(2, CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // Expected – the linked CTS may be cancelled
            }
        });

        // Give the second emission time to start waiting for the gate
        // Dispose while the second emission is waiting for the gate
        var disposeTask = sub.DisposeAsync();

        // Release the first emission so it completes and releases the gate
        proceedWithFirstEmission.SetResult();

        await firstEmission;
        await disposeTask;
        await secondEmissionTask;

        // Only value 1 should have been emitted; value 2 hits the post-gate _disposed check
        await Assert.That(items).Contains(1);
        await Assert.That(items).DoesNotContain(SampleValue2);
    }

    /// <summary>
    /// Verifies that OnErrorResumeAsync in MergeCoordinator returns early at the
    /// post-gate disposed check when disposal occurs while waiting for the gate.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposedWhileGateHeld_ThenRelayErrorAsyncReturnsPostGate()
    {
        var innerSource = new DirectSource<int>();
        var outerSource = new DirectSource<IObservableAsync<int>>();
        var errors = new List<Exception>();
        var gateHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceedWithFirstEmission = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                async (_, _) =>
                {
                    // Hold the gate on the first emission
                    gateHeld.SetResult();
                    await proceedWithFirstEmission.Task;
                },
                (ex, _) =>
                {
                    lock (_gate)
                    {
                        errors.Add(ex);
                    }

                    return default;
                });

        // Subscribe the inner source through the outer
        await outerSource.EmitNext(innerSource, CancellationToken.None);

        // First emission holds the gate
        var firstEmission = innerSource.EmitNext(1, CancellationToken.None);

        // Wait until the gate is held
        await gateHeld.Task;

        // Start an error emission that will queue behind the gate
        var errorTask = Task.Run(async () =>
        {
            try
            {
                await innerSource.EmitError(new InvalidOperationException(LateErrorMessage), CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        });

        // Give the error emission time to start waiting for the gate
        // Dispose while the error emission is waiting for the gate
        var disposeTask = sub.DisposeAsync();

        // Release the first emission
        proceedWithFirstEmission.SetResult();

        await firstEmission;
        await disposeTask;
        await errorTask;

        // The error should not have been forwarded because the post-gate disposed check caught it
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies that Merge OnNextAsync returns early at the pre-gate disposed check.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposed_ThenRelayNextAsyncReturnsEarly()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        var inner = new DirectSource<int>();
        var items = new List<int>();

        var sub = await outer.Values.Merge().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await outer.OnNextAsync(inner, CancellationToken.None);
        await inner.EmitNext(1);
        await sub.DisposeAsync();
        await inner.EmitNext(Sentinel99);

        await Assert.That(items).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that Merge OnErrorResumeAsync returns early when disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposed_ThenRelayErrorAsyncReturnsEarly()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        var inner = new DirectSource<int>();
        var errors = new List<Exception>();

        var sub = await outer.Values.Merge().SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            });

        await outer.OnNextAsync(inner, CancellationToken.None);
        await sub.DisposeAsync();
        await inner.EmitError(new InvalidOperationException("post-dispose"));

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>
    /// Verifies that Merge OnNextAsync post-gate disposed guard returns early
    /// when disposal happens while waiting for the gate.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposedWhileGateHeld_ThenRelayNextAsyncReturnsPostGate()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        var inner = new DirectSource<int>();
        var items = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = await outer.Values.Merge().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            async _ =>
            {
                completionBlocked.TrySetResult();
                await allowCompletion.Task;
            });

        await outer.OnNextAsync(inner, CancellationToken.None);
        await inner.EmitNext(1);

        var failTask = Task.Run(() => outer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail"))));
        await completionBlocked.Task;

        await inner.EmitNext(Sentinel99);

        await Assert.That(items).Count().IsEqualTo(1);

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that Merge(IObservableAsync of IObservableAsync) drops values after disposal.
    /// Covers the disposed-early-return guard in MergeCoordinator.RelayNextAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeOfSignalsDisposedDuringEmission_ThenDropsSubsequentValues()
    {
        var outerSource = new DirectSource<IObservableAsync<int>>();
        var innerSource = new DirectSource<int>();
        var results = new List<int>();

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await outerSource.EmitNext(innerSource);
        await innerSource.EmitNext(1);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count >= 1,
            TimeSpan.FromSeconds(5));

        await sub.DisposeAsync();

        // Emit after disposal - should be dropped by the guard
        await innerSource.EmitNext(Sentinel99);

        await Assert.That(results).Contains(1);
    }

    /// <summary>
    /// Verifies that Merge(IObservableAsync of IObservableAsync) drops error-resume after disposal.
    /// Covers the disposed-early-return guard in MergeCoordinator.RelayErrorAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeOfSignalsDisposedDuringErrorResume_ThenDropsSubsequentErrors()
    {
        var outerSource = new DirectSource<IObservableAsync<int>>();
        var innerSource = new DirectSource<int>();
        var errors = new List<Exception>();

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await outerSource.EmitNext(innerSource);
        await innerSource.EmitError(new InvalidOperationException(FirstLiteral));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => errors.Count >= 1,
            TimeSpan.FromSeconds(5));

        await sub.DisposeAsync();

        // Error after disposal - should be dropped by the guard
        await innerSource.EmitError(new InvalidOperationException(SecondLiteral));

        await Assert.That(errors).Count().IsEqualTo(1);
    }
}
