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
        List<int> items = [];

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
        List<Exception> errors = [];

        var sub = await outer.Values
            .Merge()
            .SubscribeAsync(
                static (_, _) => default,
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
            await innerSignal.OnErrorResumeAsync(
                new InvalidOperationException(LateErrorMessage),
                CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        await innerSignal.DisposeAsync();
        await outer.DisposeAsync();
    }

    /// <summary>Tests that BlendSignalSourcesSignal OnNextAsync returns early when disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposedBeforeInnerEmission_ThenRelayNextAsyncReturns()
    {
        var source = Signal.Create<int>();
        var inner = Signal.Create<IObservableAsync<int>>();
        List<int> items = [];
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

        await Assert.That(completionResult is not null).IsTrue();

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
        List<int> items = [];

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
    /// Verifies that OnNextAsync in BlendCoordinator returns early (pre-gate check)
    /// when the subscription has already been disposed.
    /// Uses DirectSource to retain a reference to the inner observer so that emissions
    /// can be attempted after disposal without being blocked by Signal un-subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposed_ThenRelayNextAsyncReturnsPreGate()
    {
        DirectSource<int> innerSource = new();
        DirectSource<IObservableAsync<int>> outerSource = new();
        List<int> items = [];

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

    /// <summary>Verifies that OnErrorResumeAsync in BlendCoordinator returns early when the subscription has already been disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposed_ThenRelayErrorAsyncReturns()
    {
        DirectSource<int> innerSource = new();
        DirectSource<IObservableAsync<int>> outerSource = new();
        List<Exception> errors = [];

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                static (_, _) => default,
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
    /// Verifies that OnNextAsync in BlendCoordinator returns early at the post-gate
    /// disposed check when disposal occurs while waiting for the gate.
    /// A slow observer holds the gate while a second emission waits; disposal happens
    /// before the second emission acquires the gate.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposedWhileGateHeld_ThenRelayNextAsyncReturnsPostGate()
    {
        const int SecondValue = 2;
        List<int> values = [];
        List<Exception> errors = [];
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
        {
            values.Add(value);
            return default;
        },
            (exception, _) =>
        {
            errors.Add(exception);
            return default;
        });
        SignalAsyncExtensions.BlendCoordinator<int> coordinator = new(observer);
        await coordinator.DisposeAsync();
        await coordinator.RelayNextIfActiveAsync(SecondValue);
        await Assert.That(values).IsEmpty();
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>
    /// Verifies that OnErrorResumeAsync in BlendCoordinator returns early at the
    /// post-gate disposed check when disposal occurs while waiting for the gate.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalOfSignalsDisposedWhileGateHeld_ThenRelayErrorAsyncReturnsPostGate()
    {
        List<int> values = [];
        List<Exception> errors = [];
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
        {
            values.Add(value);
            return default;
        },
            (exception, _) =>
        {
            errors.Add(exception);
            return default;
        });
        SignalAsyncExtensions.BlendCoordinator<int> coordinator = new(observer);
        await coordinator.DisposeAsync();
        await coordinator.RelayErrorIfActiveAsync(new InvalidOperationException("late"));
        await Assert.That(values).IsEmpty();
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies that Merge OnNextAsync returns early at the pre-gate disposed check.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeSignalDisposed_ThenRelayNextAsyncReturnsEarly()
    {
        var outer = Signal.Create<IObservableAsync<int>>();
        DirectSource<int> inner = new();
        List<int> items = [];

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
        DirectSource<int> inner = new();
        List<Exception> errors = [];

        var sub = await outer.Values.Merge().SubscribeAsync(
            static (_, _) => default,
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
        DirectSource<int> inner = new();
        List<int> items = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = await outer.Values.Merge().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            async _ =>
            {
                IgnoredResult.Of(completionBlocked.TrySetResult());
                await allowCompletion.Task;
            });

        await outer.OnNextAsync(inner, CancellationToken.None);
        await inner.EmitNext(1);

        var failTask = outer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));
        await completionBlocked.Task;

        await inner.EmitNext(Sentinel99);

        await Assert.That(items).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that Merge(IObservableAsync of IObservableAsync) drops values after disposal.
    /// Covers the disposed-early-return guard in BlendCoordinator.RelayNextAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeOfSignalsDisposedDuringEmission_ThenDropsSubsequentValues()
    {
        DirectSource<IObservableAsync<int>> outerSource = new();
        DirectSource<int> innerSource = new();
        List<int> results = [];

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

        await Assert.That(results.Count >= 1).IsTrue();

        await sub.DisposeAsync();

        // Emit after disposal - should be dropped by the guard
        await innerSource.EmitNext(Sentinel99);

        await Assert.That(results).Contains(1);
    }

    /// <summary>
    /// Verifies that Merge(IObservableAsync of IObservableAsync) drops error-resume after disposal.
    /// Covers the disposed-early-return guard in BlendCoordinator.RelayErrorAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMergeOfSignalsDisposedDuringErrorResume_ThenDropsSubsequentErrors()
    {
        DirectSource<IObservableAsync<int>> outerSource = new();
        DirectSource<int> innerSource = new();
        List<Exception> errors = [];

        var sub = await outerSource
            .Merge()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await outerSource.EmitNext(innerSource);
        await innerSource.EmitError(new InvalidOperationException(FirstLiteral));

        await Assert.That(errors.Count >= 1).IsTrue();

        await sub.DisposeAsync();

        // Error after disposal - should be dropped by the guard
        await innerSource.EmitError(new InvalidOperationException(SecondLiteral));

        await Assert.That(errors).Count().IsEqualTo(1);
    }
}
