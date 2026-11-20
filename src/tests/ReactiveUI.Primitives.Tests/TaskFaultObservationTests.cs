// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="TaskFaultObservation"/>.</summary>
public sealed class TaskFaultObservationTests
{
    /// <summary>The number of cancelled tasks handled by the cancellation test.</summary>
    private const int CancelledTaskCount = 2;

    /// <summary>Verifies a null task is rejected when the call is made, rather than faulting the returned task.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task HandleCancellationAsync_NullTask_ThrowsArgumentNullException()
    {
        await Assert.That(static () => TaskFaultObservation.HandleCancellationAsync(null!, null))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => TaskFaultObservation.HandleCancellationAsync<int>(null!, null))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies a cancelled task runs the action and completes normally, returning the default for the result overload.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task HandleCancellationAsync_CancelledTask_InvokesActionAndCompletes()
    {
        var calls = 0;
        await TaskFaultObservation.HandleCancellationAsync(Task.FromCanceled(new(canceled: true)), () => calls++);
        var result = await TaskFaultObservation.HandleCancellationAsync(Task.FromCanceled<int>(new(canceled: true)), () => calls++);

        await Assert.That(calls).IsEqualTo(CancelledTaskCount);
        await Assert.That(result).IsEqualTo(default(int));
    }
}
