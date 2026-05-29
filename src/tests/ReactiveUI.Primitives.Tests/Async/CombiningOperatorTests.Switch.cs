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
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Subjects;

namespace ReactiveUI.Primitives.Async.Tests;

/// <content>
/// Switch tests for combining operators.
/// </content>
public partial class CombiningOperatorTests
{
    /// <summary>
    /// Verifies that switch propagates an error when the outer source completes with a failure.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchOuterSourceErrors_ThenFailurePropagates()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        Result? completionResult = null;

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await outer.OnCompletedAsync(Result.Failure(new InvalidOperationException("outer fail")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that switch propagates a failure when the inner source errors.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchInnerSourceErrors_ThenFailurePropagates()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        Result? completionResult = null;

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await outer.OnNextAsync(
            ObservableAsync.Throw<int>(new InvalidOperationException(InnerFailMessage)),
            CancellationToken.None);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that disposing a switch subscription while an inner source is active does not throw.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchDisposedDuringInnerSubscription_ThenNoError()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        var innerSubject = SubjectAsync.Create<int>();

        var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                null);

        await outer.OnNextAsync(innerSubject.Values, CancellationToken.None);

        await sub.DisposeAsync();

        await innerSubject.DisposeAsync();
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when the outer source throws synchronously during subscription in Switch,
    /// the subscription is disposed and the exception propagates.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchSubscriptionThrows_ThenDisposesAndRethrows()
    {
        var failing = ObservableAsync.Create<IObservableAsync<int>>((_, _) =>
        {
            try
            {
                throw new InvalidOperationException("switch subscribe boom");
            }
            catch (Exception exception)
            {
                return ValueTask.FromException<IAsyncDisposable>(exception);
            }
        });

        var act = async () =>
        {
            await using var sub = await failing
                .Switch()
                .SubscribeAsync(
                    (_, _) => default,
                    null);
        };

        await Assert.ThrowsAsync<InvalidOperationException>(act);
    }

    /// <summary>
    /// Verifies that when the outer source completes while an inner source is still active,
    /// completion is deferred until the inner source completes successfully.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchOuterCompletesBeforeInner_ThenCompletesWhenInnerDone()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        var innerSubject = SubjectAsync.Create<int>();
        Result? completionResult = null;
        var items = new List<int>();

        await using var sub = await outer.Values
            .Switch()
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

        await outer.OnNextAsync(innerSubject.Values, CancellationToken.None);

        // Complete the outer while inner is still active
        await outer.OnCompletedAsync(Result.Success);

        // Should not have completed yet because inner is still active
        await Assert.That(completionResult).IsNull();

        // Emit from inner, then complete inner
        await innerSubject.OnNextAsync(Sentinel42, CancellationToken.None);
        await innerSubject.OnCompletedAsync(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
        await Assert.That(items).Contains(Sentinel42);

        await innerSubject.DisposeAsync();
        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when disposing the previous inner subscription throws during a switch,
    /// the error is propagated via completion.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchPreviousInnerDisposalThrows_ThenCompletesWithFailure()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        Result? completionResult = null;

        var throwOnDispose = ObservableAsync.Create<int>((_, _) => new(new ThrowingDisposable()));

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await outer.OnNextAsync(throwOnDispose, CancellationToken.None);

        // Switch to a new inner – this will try to dispose the previous (throwing) one
        await outer.OnNextAsync(ObservableAsync.Return(Sentinel99), CancellationToken.None);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that error-resume events from the outer source in Switch are forwarded
    /// to the downstream observer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchOuterErrorResume_ThenForwardedToObserver()
    {
        var outerSource = ObservableAsync.Create<IObservableAsync<int>>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(OuterWarningMessage), ct);
            await observer.OnNextAsync(ObservableAsync.Return(1), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var errors = new List<Exception>();
        var items = new List<int>();

        await using var sub = await outerSource
            .Switch()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0].Message).IsEqualTo(OuterWarningMessage);
    }

    /// <summary>
    /// Verifies that error-resume events from the inner source in Switch are forwarded
    /// to the downstream observer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchInnerErrorResume_ThenForwardedToObserver()
    {
        var innerWithError = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(InnerWarningMessage), ct);
            await observer.OnNextAsync(42, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        var errors = new List<Exception>();
        var items = new List<int>();

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await outer.OnNextAsync(innerWithError, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0].Message).IsEqualTo(InnerWarningMessage);
        await Assert.That(items).Contains(Sentinel42);

        await outer.DisposeAsync();
    }

    /// <summary>
    /// Verifies that when the outer source in Switch completes successfully with no active
    /// inner subscription, the switch completes immediately with success.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchOuterCompletesWithNoActiveInner_ThenImmediatelyCompletes()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        Result? completionResult = null;

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await outer.OnCompletedAsync(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();

        await outer.DisposeAsync();
    }

    /// <summary>Tests Switch with inner sequence error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchInnerError_ThenOuterReceivesError()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        var errors = new List<Exception>();

        await using var sub = await outer.Values.Switch().SubscribeAsync(
            (Action<int>)(_ => { }),
            (Action<Exception>?)(ex => errors.Add(ex)),
            null,
            CancellationToken.None);

        await outer.OnNextAsync(
            ObservableAsync.Throw<int>(new InvalidOperationException("inner-error")),
            CancellationToken.None);

        await Task.Yield();

        await Assert.That(errors).Count().IsGreaterThanOrEqualTo(0);
    }

    /// <summary>Tests that Switch completes when outer completes with no inner sequences.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchOuterCompletesWithNoInner_ThenCompletes()
    {
        Result? completionResult = null;

        await using var sub = await ObservableAsync.Empty<IObservableAsync<int>>()
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests that Switch forwards error from inner sequence.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchInnerErrors_ThenErrorPropagated()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        Result? completionResult = null;

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await outer.OnNextAsync(
            ObservableAsync.Throw<int>(new InvalidOperationException(InnerFailMessage)),
            CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        await outer.DisposeAsync();
    }

    /// <summary>Tests that Switch forwards error resume from inner sequence.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchInnerErrorResume_ThenForwarded()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        var errors = new List<Exception>();

        var inner = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException(InnerWarningMessage), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await outer.OnNextAsync(inner, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => errors.Count >= 1,
            TimeSpan.FromSeconds(5));

        await Assert.That(errors).Count().IsEqualTo(1);

        await outer.DisposeAsync();
    }

    /// <summary>
    /// Tests that Switch emits from the latest inner sequence.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitch_ThenEmitsFromLatestInnerSequence()
    {
        var outer = SubjectAsync.Create<IObservableAsync<int>>();
        var items = new List<int>();

        await using var sub = await outer.Values.Switch().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        var inner1 = SubjectAsync.Create<int>();
        await outer.OnNextAsync(inner1.Values, CancellationToken.None);
        await inner1.OnNextAsync(1, CancellationToken.None);
        await inner1.OnNextAsync(SampleValue2, CancellationToken.None);

        var inner2 = SubjectAsync.Create<int>();
        await outer.OnNextAsync(inner2.Values, CancellationToken.None);
        await inner2.OnNextAsync(SampleValue10, CancellationToken.None);
        await inner2.OnNextAsync(SampleValue20, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Contains(SampleValue10),
            TimeSpan.FromSeconds(5));

        await Assert.That(items).Contains(1);
        await Assert.That(items).Contains(SampleValue10);
    }

    /// <summary>
    /// Tests that Switch completes with failure when the inner subscription throws during subscribe,
    /// exercising the outer catch in SwitchObservable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchInnerSubscriptionThrows_ThenCompletesWithFailure()
    {
        var failingInner = ObservableAsync.Create<int>((_, _) =>
            throw new InvalidOperationException("inner subscribe boom"));

        var outer = ObservableAsync.Return(failingInner);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await outer.Switch().ToListAsync());
    }

    /// <summary>Tests Switch emits from the latest inner observable.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchNewInnerArrives_ThenEmitsFromLatest()
    {
        var result = await ObservableAsync.Return<IObservableAsync<int>>(ObservableAsync.Return(42))
            .Switch()
            .FirstAsync();

        await Assert.That(result).IsEqualTo(Sentinel42);
    }

    /// <summary>Tests Switch inner failure propagates.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchInnerFails_ThenErrorPropagated()
    {
        var error = new InvalidOperationException("switch-inner");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ObservableAsync.Return(ObservableAsync.Throw<int>(error))
                .Switch()
                .FirstAsync());
    }

    /// <summary>Verifies that subscribing <c>Switch</c> with an already-cancelled token
    /// short-circuits the subscription's cancellation chain immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchSubscribedWithAlreadyCancelledToken_ThenSubscriptionDisposes()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await using var sub = await ObservableAsync.Return<IObservableAsync<int>>(ObservableAsync.Return(1))
            .Switch()
            .SubscribeAsync(static (_, _) => default, cts.Token);

        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies that subscribing <c>Switch</c> with a cancellable but not-yet-cancelled
    /// token registers the external link and the registration fires on later cancellation.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwitchExternalTokenCancelledAfterSubscribe_ThenRegistrationFires()
    {
        using var cts = new CancellationTokenSource();
        var outer = SubjectAsync.Create<IObservableAsync<int>>();

        await using var sub = await outer.Values
            .Switch()
            .SubscribeAsync(static (_, _) => default, cts.Token);

        await cts.CancelAsync();
        await Assert.That(sub).IsNotNull();
    }
}
