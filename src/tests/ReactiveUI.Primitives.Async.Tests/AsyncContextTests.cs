// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests continuation dispatch through asynchronous contexts.</summary>
public sealed class AsyncContextTests
{
    /// <summary>A forced default-context switch dispatches its continuation to the thread pool.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SwitchContextAsync_DefaultContext_QueuesContinuation()
    {
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var awaiter = AsyncContext.Default.SwitchContextAsync(true, CancellationToken.None).GetAwaiter();

        await Assert.That(awaiter.IsCompleted).IsFalse();
        awaiter.OnCompleted(() => completed.SetResult(Thread.CurrentThread.IsThreadPoolThread));

        await Assert.That(await completed.Task).IsTrue();
        awaiter.GetResult();
    }
}
