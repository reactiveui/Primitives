// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Publication guards for builder overflow tests.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The first publication phase.</summary>
    private const string FirstOverflowPublication = "first publication";

    /// <summary>The second publication phase.</summary>
    private const string SecondOverflowPublication = "second publication";

    /// <summary>The third publication phase.</summary>
    private const string ThirdOverflowPublication = "third publication";

    /// <summary>The fourth publication phase.</summary>
    private const string FourthOverflowPublication = "fourth publication";

    /// <summary>Verifies timeout evidence preserves the original failure without waiting for a guard to expire.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OverflowPublicationTimeoutIncludesPhaseAndThreadPoolSnapshot()
    {
        var original = new TimeoutException("Publication timed out.");
        var publication = Task.FromException<PublishReceipt>(original);
        var failure = await Assert.ThrowsExactlyAsync<TimeoutException>(
            () => AwaitOverflowPublicationAsync(
                publication,
                nameof(OverflowPublicationTimeoutIncludesPhaseAndThreadPoolSnapshot),
                "second publication"));

        var message = failure?.Message;
        await Assert.That(failure?.InnerException).IsSameReferenceAs(original);
        await Assert.That(message).Contains(nameof(OverflowPublicationTimeoutIncludesPhaseAndThreadPoolSnapshot));
        await Assert.That(message).Contains("second publication");
        await Assert.That(message).Contains("Task.Status=Faulted");
        await Assert.That(message).Contains("ThreadPool.ThreadCount=");
        await Assert.That(message).Contains("PendingWorkItemCount=");
        await Assert.That(message).Contains("CompletedWorkItemCount=");
        await Assert.That(message).Contains("AvailableWorkerThreads=");
        await Assert.That(message).Contains("MaxWorkerThreads=");
        await Assert.That(message).Contains("GuardTimeout=00:00:05");
    }

    /// <summary>Verifies the publication guard preserves capacity failures.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task OverflowPublicationGuardPreservesCapacityFailure()
    {
        var original = new QueueCapacityExceededException("Outbox is full.");
        var failure = await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => AwaitOverflowPublicationAsync(
                Task.FromException<PublishReceipt>(original),
                nameof(OverflowPublicationGuardPreservesCapacityFailure),
                "third publication"));

        await Assert.That(failure).IsSameReferenceAs(original);
    }

    /// <summary>Awaits a publication with the fixture guard and adds synchronous evidence only on timeout.</summary>
    /// <param name="publication">The publication task.</param>
    /// <param name="testName">The test awaiting the publication.</param>
    /// <param name="phase">The publication phase.</param>
    /// <returns>The publication receipt.</returns>
    /// <exception cref="TimeoutException">The publication or its guard timed out.</exception>
    private static async Task<PublishReceipt> AwaitOverflowPublicationAsync(
        Task<PublishReceipt> publication,
        string testName,
        string phase)
    {
        try
        {
            return await publication.WaitAsync(GuardTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException failure)
        {
            throw CreateOverflowPublicationTimeout(publication, testName, phase, failure);
        }
    }

    /// <summary>Captures immediate task and thread pool evidence for a timeout.</summary>
    /// <param name="publication">The publication task.</param>
    /// <param name="testName">The test awaiting the publication.</param>
    /// <param name="phase">The publication phase.</param>
    /// <param name="failure">The original timeout.</param>
    /// <returns>The timeout with its original exception preserved.</returns>
    private static TimeoutException CreateOverflowPublicationTimeout(
        Task<PublishReceipt> publication,
        string testName,
        string phase,
        TimeoutException failure)
    {
        ThreadPool.GetAvailableThreads(out var availableWorkers, out var availableIo);
        ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
        var message = $"{testName}: {phase} timed out while awaiting publication. "
            + $"GuardTimeout={GuardTimeout}; Task.Status={publication.Status}; "
            + $"ThreadPool.ThreadCount={ThreadPool.ThreadCount}; "
            + $"PendingWorkItemCount={ThreadPool.PendingWorkItemCount}; "
            + $"CompletedWorkItemCount={ThreadPool.CompletedWorkItemCount}; "
            + $"AvailableWorkerThreads={availableWorkers}; MaxWorkerThreads={maxWorkers}; "
            + $"AvailableIoThreads={availableIo}; MaxIoThreads={maxIo}.";
        return new(message, failure);
    }
}
