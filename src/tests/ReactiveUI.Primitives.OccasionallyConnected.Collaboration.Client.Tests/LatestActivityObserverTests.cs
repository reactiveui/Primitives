// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Tests for <see cref="LatestActivityObserver"/>.</summary>
public sealed class LatestActivityObserverTests
{
    /// <summary>The first status value used by waiter ownership tests.</summary>
    private const string FirstStatus = "first";

    /// <summary>The second status value used by waiter ownership tests.</summary>
    private const string SecondStatus = "second";

    /// <summary>The terminal error message used by terminal-state tests.</summary>
    private const string TerminalErrorMessage = "terminal failure";

    /// <summary>Verifies a matching latest view is returned without registering a waiter.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WaitForAsyncReturnsMatchingLatestView()
    {
        var observer = new LatestActivityObserver();
        observer.OnNext(CreateView(FirstStatus));

        var result = await observer.WaitForAsync(
                static value => string.Equals(value.Status, FirstStatus, StringComparison.Ordinal),
                CancellationToken.None)
            .ConfigureAwait(false);

        await Assert.That(result.Status).IsEqualTo(FirstStatus);
    }

    /// <summary>Verifies an error observed before a wait is stored and thrown to later waiters.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WaitForAsyncThrowsTerminalErrorObservedBeforeWait()
    {
        var observer = new LatestActivityObserver();
        observer.OnError(new InvalidOperationException(TerminalErrorMessage));
        observer.OnNext(CreateView(SecondStatus));

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            observer.WaitForAsync(static _ => true, CancellationToken.None));

        await Assert.That(exception?.Message).IsEqualTo(TerminalErrorMessage);
    }

    /// <summary>Verifies completion before a wait is treated as terminal state.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WaitForAsyncThrowsWhenObservationCompletedBeforeWait()
    {
        var observer = new LatestActivityObserver();
        observer.OnCompleted();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            observer.WaitForAsync(static _ => true, CancellationToken.None));

        await Assert.That(exception?.Message.Contains("completed", StringComparison.OrdinalIgnoreCase) ?? false).IsTrue();
    }

    /// <summary>Verifies completion fails an active waiter.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WaitForAsyncThrowsWhenObservationCompletesDuringWait()
    {
        var observer = new LatestActivityObserver();
        var wait = observer.WaitForAsync(static _ => true, CancellationToken.None);

        observer.OnCompleted();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => wait);
        await Assert.That(exception?.Message.Contains("completed", StringComparison.OrdinalIgnoreCase) ?? false).IsTrue();
    }

    /// <summary>Verifies canceling one wait leaves the observer reusable for a later wait.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WaitForAsyncCanWaitAgainAfterCancellation()
    {
        var observer = new LatestActivityObserver();
        using var firstCancellation = new CancellationTokenSource();
        var first = observer.WaitForAsync(static value => string.Equals(value.Status, FirstStatus, StringComparison.Ordinal), firstCancellation.Token);
        await firstCancellation.CancelAsync().ConfigureAwait(false);
        _ = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => first);
        var second = observer.WaitForAsync(static value => string.Equals(value.Status, SecondStatus, StringComparison.Ordinal), CancellationToken.None);

        observer.OnNext(CreateView(FirstStatus));
        observer.OnNext(CreateView(SecondStatus));
        var result = await second.ConfigureAwait(false);

        await Assert.That(result.Status).IsEqualTo(SecondStatus);
    }

    /// <summary>Verifies only one pending wait is accepted.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WaitForAsyncRejectsSecondPendingWaiter()
    {
        var observer = new LatestActivityObserver();
        using var cancellation = new CancellationTokenSource();
        var first = observer.WaitForAsync(static value => string.Equals(value.Status, FirstStatus, StringComparison.Ordinal), cancellation.Token);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            observer.WaitForAsync(static value => string.Equals(value.Status, SecondStatus, StringComparison.Ordinal), CancellationToken.None));
        await cancellation.CancelAsync().ConfigureAwait(false);
        _ = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => first);

        await Assert.That(exception?.Message.Contains("Only one", StringComparison.Ordinal) ?? false).IsTrue();
    }

    /// <summary>Verifies a later observer error does not replace the original terminal cause.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OnErrorPreservesFirstTerminalCause()
    {
        var observer = new LatestActivityObserver();
        var original = new InvalidOperationException(TerminalErrorMessage);
        observer.OnError(original);
        observer.OnError(new InvalidOperationException("later failure"));

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() =>
            observer.WaitForAsync(static _ => true, CancellationToken.None));

        await Assert.That(ReferenceEquals(exception, original)).IsTrue();
    }

    /// <summary>Creates an activity view with the requested status.</summary>
    /// <param name="status">The status.</param>
    /// <returns>The activity view.</returns>
    private static ActivityView CreateView(string status) =>
        ActivityView.Empty() with { Status = status };
}
