// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the signal each TakeUntil overload builds for default options and cancellation tokens.</summary>
public partial class TakeUntilOperatorTests
{
    /// <summary>Observable overloads wrap in a cancellation stop only for a cancellable token, and accept default options.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTakeUntilObservableOverloadsReceiveDefaults_ThenBuildMatchingSignal()
    {
        using CancellationTokenSource cancellation = new();
        var source = SignalAsync.Never<int>();
        var other = SignalAsync.Never<int>();
        var options = TakeUntilOptions.Default;

        await Assert.That(source.TakeUntil(other, (TakeUntilOptions?)null)).IsTypeOf<SignalAsyncExtensions.TakeUntilAsyncSignal<int, int>>();
        await Assert.That(source.TakeUntil(other, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.TakeUntilAsyncSignal<int, int>>();
        await Assert.That(source.TakeUntil(other, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
        await Assert.That(source.TakeUntil(other, null, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.TakeUntilAsyncSignal<int, int>>();
        await Assert.That(source.TakeUntil(other, options, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.TakeUntilAsyncSignal<int, int>>();
        await Assert.That(source.TakeUntil(other, null, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
        await Assert.That(source.TakeUntil(other, options, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
    }

    /// <summary>Task overloads wrap in a cancellation stop only for a cancellable token, and accept default options.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTakeUntilTaskOverloadsReceiveDefaults_ThenBuildMatchingSignal()
    {
        using CancellationTokenSource cancellation = new();
        var source = SignalAsync.Never<int>();
        var task = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously).Task;
        var options = TakeUntilOptions.Default;

        await Assert.That(source.TakeUntil(task, (TakeUntilOptions?)null)).IsTypeOf<SignalAsyncExtensions.TaskStopSignal<int>>();
        await Assert.That(source.TakeUntil(task, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.TaskStopSignal<int>>();
        await Assert.That(source.TakeUntil(task, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
        await Assert.That(source.TakeUntil(task, null, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.TaskStopSignal<int>>();
        await Assert.That(source.TakeUntil(task, options, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.TaskStopSignal<int>>();
        await Assert.That(source.TakeUntil(task, null, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
        await Assert.That(source.TakeUntil(task, options, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
    }

    /// <summary>Predicate overloads wrap in a cancellation stop only for a cancellable token.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTakeUntilPredicateOverloadsReceiveTokens_ThenBuildMatchingSignal()
    {
        using CancellationTokenSource cancellation = new();
        var source = SignalAsync.Never<int>();

        await Assert.That(source.TakeUntil(static value => value > SecondItem, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.PredicateStopSignal<int>>();
        await Assert.That(source.TakeUntil(static value => value > SecondItem, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
        await Assert.That(source.TakeUntil(static (_, _) => new ValueTask<bool>(false), CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.AsyncPredicateStopSignal<int>>();
        await Assert.That(source.TakeUntil(static (_, _) => new ValueTask<bool>(false), cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
    }

    /// <summary>Completion-delegate overloads wrap in a cancellation stop only for a cancellable token, and accept default options.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTakeUntilDelegateOverloadsReceiveDefaults_ThenBuildMatchingSignal()
    {
        using CancellationTokenSource cancellation = new();
        var source = SignalAsync.Never<int>();
        CompletionSignalDelegate stopSignal = static _ => DisposableAsync.Empty;
        var options = TakeUntilOptions.Default;

        await Assert.That(source.TakeUntil(stopSignal, (TakeUntilOptions?)null)).IsTypeOf<SignalAsyncExtensions.DelegateStopSignal<int>>();
        await Assert.That(source.TakeUntil(stopSignal, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.DelegateStopSignal<int>>();
        await Assert.That(source.TakeUntil(stopSignal, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
        await Assert.That(source.TakeUntil(stopSignal, null, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.DelegateStopSignal<int>>();
        await Assert.That(source.TakeUntil(stopSignal, options, CancellationToken.None)).IsTypeOf<SignalAsyncExtensions.DelegateStopSignal<int>>();
        await Assert.That(source.TakeUntil(stopSignal, null, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
        await Assert.That(source.TakeUntil(stopSignal, options, cancellation.Token)).IsTypeOf<SignalAsyncExtensions.CancellationStopSignal<int>>();
    }
}
