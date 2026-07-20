// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Verifies <see cref="ThreadPoolSequencer"/> forwards to System.Reactive's thread-pool scheduler.</summary>
public class ThreadPoolSequencerTests
{
    /// <summary>Verifies the exposed instance is System.Reactive's thread-pool scheduler singleton.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenInstanceRead_ThenItIsTheThreadPoolScheduler() =>
        await Assert.That(ThreadPoolSequencer.Instance)
            .IsSameReferenceAs(System.Reactive.Concurrency.ThreadPoolScheduler.Instance);
}
