// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the cancellation-token overloads of the task-returning terminal operators.</summary>
public partial class SignalOperatorParityMixinsTests
{
    /// <summary>Verifies that <c>ToTask</c> on a task returns the same instance when the token cannot be canceled.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ToTaskOnTaskWithNoneTokenReturnsSameInstance()
    {
        var task = Task.FromResult(One);

        await Assert.That(ReferenceEquals(task.ToTask(CancellationToken.None), task)).IsTrue();
    }

    /// <summary>Verifies that <c>ToTask</c> on a completed task returns the same instance even with a cancelable token.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ToTaskOnCompletedTaskReturnsSameInstance()
    {
        using CancellationTokenSource cts = new();
        var task = Task.FromResult(Two);

        await Assert.That(ReferenceEquals(task.ToTask(cts.Token), task)).IsTrue();
    }

    /// <summary>Verifies that <c>ToTask</c> on a task validates a null receiver.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ToTaskOnNullTaskThrowsArgumentNull()
    {
        await Assert.That(() => ((Task<int>)null!).ToTask(CancellationToken.None)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies that <c>ToTask</c> on a pending task returns a canceled task for a pre-canceled token.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ToTaskOnPendingTaskWithPreCanceledTokenIsCanceled()
    {
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();
        TaskCompletionSource<int> pending = new();

        var task = pending.Task.ToTask(cts.Token);

        await Assert.That(task.IsCanceled).IsTrue();
    }

    /// <summary>Verifies the issue #108 scenario: <c>FirstAsync().ToTask(token)</c> cancels when the token fires before a value arrives.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FirstAsyncToTaskCancelsWhenTokenFires()
    {
        using CancellationTokenSource cts = new();
        Signal<string> messageStream = new();

        var task = messageStream.Keep(message => message.StartsWith("ERROR", StringComparison.Ordinal)).FirstAsync().ToTask(cts.Token);
        await cts.CancelAsync();
        Task pending = task;

        await Assert.That(() => pending).Throws<TaskCanceledException>();
    }

    /// <summary>Verifies the issue #108 scenario: <c>FirstAsync().ToTask(token)</c> yields the first matching value when it arrives before cancellation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FirstAsyncToTaskYieldsFirstMatchingValue()
    {
        using CancellationTokenSource cts = new();
        Signal<string> messageStream = new();

        var task = messageStream.Keep(message => message.StartsWith("ERROR", StringComparison.Ordinal)).FirstAsync().ToTask(cts.Token);
        messageStream.OnNext("info");
        messageStream.OnNext("ERROR: boom");

        await Assert.That(await task).IsEqualTo("ERROR: boom");
    }

    /// <summary>Verifies that <c>FirstAsync</c> with a token yields the first value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FirstAsyncWithTokenYieldsFirstValue()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.FirstAsync(cts.Token);
        source.OnNext(Three);
        source.OnNext(Four);

        await Assert.That(await task).IsEqualTo(Three);
    }

    /// <summary>Verifies that <c>FirstAsync</c> with a token cancels when the token fires before a value arrives.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FirstAsyncWithTokenCancelsWhenTokenFires()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.FirstAsync(cts.Token);
        await cts.CancelAsync();

        await Assert.That(() => task).Throws<TaskCanceledException>();
    }

    /// <summary>Verifies that <c>FirstAsync</c> with a pre-canceled token returns a canceled task.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FirstAsyncWithPreCanceledTokenIsCanceled()
    {
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();
        Signal<int> source = new();

        var task = source.FirstAsync(cts.Token);

        await Assert.That(task.IsCanceled).IsTrue();
    }

    /// <summary>Verifies that <c>FirstOrDefaultAsync</c> with a token uses the fallback value on an empty source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FirstOrDefaultAsyncWithTokenUsesFallbackOnEmptySource()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.FirstOrDefaultAsync(Four, cts.Token);
        source.OnCompleted();

        await Assert.That(await task).IsEqualTo(Four);
    }

    /// <summary>Verifies that <c>FirstOrDefaultAsync</c> with a token cancels when the token fires.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FirstOrDefaultAsyncWithTokenCancelsWhenTokenFires()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.FirstOrDefaultAsync(cts.Token);
        await cts.CancelAsync();

        await Assert.That(() => task).Throws<TaskCanceledException>();
    }

    /// <summary>Verifies that <c>LastAsync</c> with a token yields the final value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LastAsyncWithTokenYieldsFinalValue()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.LastAsync(cts.Token);
        source.OnNext(One);
        source.OnNext(Two);
        source.OnCompleted();

        await Assert.That(await task).IsEqualTo(Two);
    }

    /// <summary>Verifies that <c>LastAsync</c> with a token cancels when the token fires.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LastAsyncWithTokenCancelsWhenTokenFires()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.LastAsync(cts.Token);
        await cts.CancelAsync();

        await Assert.That(() => task).Throws<TaskCanceledException>();
    }

    /// <summary>Verifies that <c>LastOrDefaultAsync</c> with a token uses the fallback value on an empty source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LastOrDefaultAsyncWithTokenUsesFallbackOnEmptySource()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.LastOrDefaultAsync(Three, cts.Token);
        source.OnCompleted();

        await Assert.That(await task).IsEqualTo(Three);
    }

    /// <summary>Verifies that <c>LastOrDefaultAsync</c> with a token yields the default value on an empty source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LastOrDefaultAsyncWithTokenYieldsDefaultOnEmptySource()
    {
        using CancellationTokenSource cts = new();
        Signal<int> source = new();

        var task = source.LastOrDefaultAsync(cts.Token);
        source.OnCompleted();

        await Assert.That(await task).IsEqualTo(0);
    }

    /// <summary>Verifies that the cancellation-token overloads validate a null source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CancellationOverloadsValidateNullSource()
    {
        await Assert.That(() => ((IObservable<int>)null!).FirstAsync(CancellationToken.None)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => ((IObservable<int>)null!).FirstOrDefaultAsync(CancellationToken.None)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => ((IObservable<int>)null!).FirstOrDefaultAsync(One, CancellationToken.None)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => ((IObservable<int>)null!).LastAsync(CancellationToken.None)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => ((IObservable<int>)null!).LastOrDefaultAsync(One, CancellationToken.None)).ThrowsExactly<ArgumentNullException>();
    }
}
