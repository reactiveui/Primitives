// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Completes branch and contract coverage for factory, task, and terminal-task behavior.</summary>
public partial class SignalFactoriesTests
{
    /// <summary>Expected use-factory errors.</summary>
    private static readonly string[] ExpectedUseErrors = ["The signal factory returned null.", "resource"];

    /// <summary>Expected task error names.</summary>
    private static readonly string[] ExpectedTaskErrors =
        [nameof(TaskCanceledException), nameof(InvalidOperationException)];

    /// <summary>Expected async error messages.</summary>
    private static readonly string[] ExpectedAsyncErrors = ["async"];

    /// <summary>Expected single async value.</summary>
    private static readonly int[] ExpectedSingleAsyncValue = [1];

    /// <summary>Exercises task, async-enumerable, and terminal task branches.</summary>
    /// <returns>A task that completes when asynchronous coverage has run.</returns>
    [Test]
    public async Task FactoriesTasksAndTerminalTasksCoverCancellationFaultAndEmptyBranches()
    {
        List<string> useErrors = [];
        List<string> taskErrors = [];
        List<int> asyncValues = [];
        List<string> asyncErrors = [];
        _ = Signal.Use(static () => EmptyDisposable.Instance, static _ => (IObservable<int>)null!)
            .Subscribe(static _ => { }, ex => useErrors.Add(ex.Message));
        _ = Signal.Use<IDisposable, int>(static () => throw new InvalidOperationException("resource"), static _ => Signal.Emit(1))
            .Subscribe(static _ => { }, ex => useErrors.Add(ex.Message));
        await ObserveTaskError(Task.FromCanceled<int>(new(true)), taskErrors);
        await ObserveTaskError(Task.FromException<int>(new InvalidOperationException("faulted")), taskErrors);

        static async IAsyncEnumerable<int> ThrowingAsyncEnumerable()
        {
            yield return 1;
            await Task.Yield();
            throw new InvalidOperationException("async");
        }

        _ = Signal.FromAsyncEnumerable(ThrowingAsyncEnumerable())
            .Subscribe(asyncValues.Add, ex => asyncErrors.Add(ex.Message));
        await TestPolling.SpinUntil(() => asyncErrors.Count == 1, TimeSpan.FromSeconds(TimeoutSeconds));
        var firstFailure = await AssertTaskFault(
            static () => Signal.None<int>().FirstAsync(),
            typeof(InvalidOperationException));
        var collectFailure = await AssertTaskFault(
            static () => Signal.Fail<int>(new InvalidOperationException("collect")).CollectArrayAsync(),
            typeof(InvalidOperationException));
        var listFailure = await AssertTaskFault(
            static () => Signal.Fail<int>(new InvalidOperationException("list")).CollectListAsync(),
            typeof(InvalidOperationException));
        await Assert.That(useErrors.SequenceEqual(ExpectedUseErrors)).IsTrue();
        await Assert.That(taskErrors.SequenceEqual(ExpectedTaskErrors)).IsTrue();
        await Assert.That(asyncValues.SequenceEqual(ExpectedSingleAsyncValue)).IsTrue();
        await Assert.That(asyncErrors.SequenceEqual(ExpectedAsyncErrors)).IsTrue();
        await Assert.That(firstFailure.Message).IsEqualTo("The source completed without producing a value.");
        await Assert.That(collectFailure.Message).IsEqualTo("collect");
        await Assert.That(listFailure.Message).IsEqualTo("list");
    }

    /// <summary>Observes the error produced by a task-backed signal.</summary>
    /// <param name="task">The source task.</param>
    /// <param name="errors">The error name sink.</param>
    /// <returns>A task that completes when the error has been observed.</returns>
    private static async Task ObserveTaskError(Task<int> task, List<string> errors)
    {
        _ = Signal.FromTask(task).Subscribe(static _ => { }, ex => errors.Add(ex.GetType().Name));
        await TestPolling.SpinUntil(() => errors.Count > 0, TimeSpan.FromSeconds(TimeoutSeconds));
    }

    /// <summary>Asserts that a task factory faults with the expected exception type.</summary>
    /// <param name="taskFactory">The task factory.</param>
    /// <param name="expectedExceptionType">The expected exception type.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="taskFactory"/> did not fault with <paramref name="expectedExceptionType"/>.</exception>
    private static async Task<Exception> AssertTaskFault(Func<Task> taskFactory, Type expectedExceptionType)
    {
        try
        {
            await taskFactory();
        }
        catch (Exception exception) when (exception.GetType() == expectedExceptionType)
        {
            return exception;
        }

        throw new InvalidOperationException($"Expected task fault {expectedExceptionType.Name}.");
    }
}
