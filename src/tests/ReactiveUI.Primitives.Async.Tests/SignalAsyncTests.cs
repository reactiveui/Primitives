// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests asynchronous signal factories using their default execution settings.</summary>
public sealed class SignalAsyncTests
{
    /// <summary>The default background-job overload forwards the job's value and completion.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task CreateAsBackgroundJob_DefaultSettings_ForwardsJobResult()
    {
        var source = SignalAsync.CreateAsBackgroundJob<int>(static async (observer, token) =>
        {
            await observer.OnNextAsync(1, token);
            await observer.OnCompletedAsync(Result.Success);
        });

        await Assert.That(await source.ToListAsync()).IsCollectionEqualTo([1]);
    }

    /// <summary>The default interval factory creates a system-clock interval.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Interval_DefaultSettings_CreatesIntervalSignal()
    {
        var source = SignalAsync.Interval(TimeSpan.FromSeconds(1));

        await Assert.That(source).IsTypeOf<IntervalSignal>();
    }
}
