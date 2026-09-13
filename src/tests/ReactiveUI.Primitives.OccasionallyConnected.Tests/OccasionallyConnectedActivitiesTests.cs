// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedActivities"/>.</summary>
[NotInParallel]
public sealed class OccasionallyConnectedActivitiesTests
{
    /// <summary>The deterministic fractional sampling ratio used by the sampler test.</summary>
    private const double HalfSamplingRatio = 0.5D;

    /// <summary>The expected number of sampler invocations in the deterministic sampler test.</summary>
    private const int ExpectedSamplerCalls = 2;

    /// <summary>The number of concurrent activity starts to perform.</summary>
    private const int ConcurrentStartCount = 64;

    /// <summary>A ratio guaranteed to reject the deterministic default trace.</summary>
    private const double TinySamplingRatio = 0.000001D;

    /// <summary>A ratio guaranteed to accept the deterministic default trace.</summary>
    private const double LargeSamplingRatio = 0.999999D;

    /// <summary>The name used by parent activities in these tests.</summary>
    private const string ParentOperationName = "parent";

    /// <summary>Verifies every defined activity has its required name, kind, and source.</summary>
    /// <param name="activityName">The bounded activity name to start.</param>
    /// <param name="expectedOperationName">The expected diagnostic operation name.</param>
    /// <param name="expectedKind">The expected activity kind.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(0, "oc.context.start", ActivityKind.Internal)]
    [Arguments(1, "oc.transport.connect", ActivityKind.Client)]
    [Arguments(2, "oc.sync.push", ActivityKind.Client)]
    [Arguments(3, "oc.sync.receive", ActivityKind.Client)]
    [Arguments(4, "oc.store.commit", ActivityKind.Internal)]
    [Arguments(5, "oc.conflict.resolve", ActivityKind.Internal)]
    [Arguments(6, "oc.store.compact", ActivityKind.Internal)]
    public async Task StartsEveryRequiredActivityWithItsDefinedMetadata(
        int activityName,
        string expectedOperationName,
        ActivityKind expectedKind)
    {
        using var listener = CreateListener();
        using var recorder = new OccasionallyConnectedActivities();

        using var activity = recorder.Start((OccasionallyConnectedActivityName)activityName);

        await Assert.That(activity).IsNotNull();
        await Assert.That(RequireActivity(activity).OperationName).IsEqualTo(expectedOperationName);
        await Assert.That(activity.Kind).IsEqualTo(expectedKind);
        await Assert.That(activity.Source.Name).IsEqualTo(OccasionallyConnectedActivities.DiagnosticSourceName);
        await Assert.That(activity.Tags.Any()).IsFalse();
    }

    /// <summary>Verifies an activity naturally inherits the current parent.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StartInheritsTheCurrentActivityAsItsParent()
    {
        using var listener = CreateListener();
        using var recorder = new OccasionallyConnectedActivities();
        using var parent = new Activity(ParentOperationName).Start();

        using var child = recorder.Start(OccasionallyConnectedActivityName.SyncPush);

        await Assert.That(child).IsNotNull();
        await Assert.That(RequireActivity(child).ParentId).IsEqualTo(RequireActivity(parent).Id);
    }

    /// <summary>Verifies disposing a started activity restores the prior current activity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposingStartedActivityRestoresThePriorCurrentActivity()
    {
        using var listener = CreateListener();
        using var recorder = new OccasionallyConnectedActivities();
        using var parent = new Activity(ParentOperationName).Start();

        var child = recorder.Start(OccasionallyConnectedActivityName.StoreCommit);

        await Assert.That(Activity.Current).IsEqualTo(child);
        RequireActivity(child).Dispose();
        await Assert.That(Activity.Current).IsEqualTo(parent);
    }

    /// <summary>Verifies disposing the recorder prevents later activity creation without disposing caller-owned activities.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeStopsFutureActivityCreation()
    {
        using var listener = CreateListener();
        var recorder = new OccasionallyConnectedActivities();
        var activity = recorder.Start(OccasionallyConnectedActivityName.StoreCompact);

        recorder.Dispose();
        recorder.Dispose();
        var afterDispose = recorder.Start(OccasionallyConnectedActivityName.StoreCompact);

        await Assert.That(activity).IsNotNull();
        await Assert.That(afterDispose).IsNull();
        RequireActivity(activity).Dispose();
    }

    /// <summary>Verifies disabled recorders do not start activities.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisabledRecorderDoesNotStartActivities()
    {
        using var listener = CreateListener();
        using var recorder = new OccasionallyConnectedActivities(enabled: false);

        var activity = recorder.Start(OccasionallyConnectedActivityName.ContextStart);

        await Assert.That(activity).IsNull();
    }

    /// <summary>Verifies sampling ratios of zero and one respectively reject and admit activities.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task BoundarySamplingRatiosRespectTheirLimits()
    {
        using var listener = CreateListener();
        using var rejected = new OccasionallyConnectedActivities(samplingRatio: 0D);
        using var admitted = new OccasionallyConnectedActivities();

        var rejectedActivity = rejected.Start(OccasionallyConnectedActivityName.ContextStart);
        using var admittedActivity = admitted.Start(OccasionallyConnectedActivityName.ContextStart);

        await Assert.That(rejectedActivity).IsNull();
        await Assert.That(admittedActivity).IsNotNull();
    }

    /// <summary>Verifies fractional sampling is deterministic for a known parent trace.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FractionalSamplingUsesDeterministicDefaultTraceSelection()
    {
        using var listener = CreateListener();
        using var rejected = new OccasionallyConnectedActivities(samplingRatio: TinySamplingRatio);
        using var admitted = new OccasionallyConnectedActivities(samplingRatio: LargeSamplingRatio);
        using var parent = new Activity(ParentOperationName)
            .SetParentId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .Start();

        var rejectedActivity = rejected.Start(OccasionallyConnectedActivityName.ContextStart);
        using var admittedActivity = admitted.Start(OccasionallyConnectedActivityName.ContextStart);

        await Assert.That(rejectedActivity).IsNull();
        await Assert.That(admittedActivity).IsNotNull();
    }

    /// <summary>Verifies invalid sampling ratios are rejected during construction.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InvalidSamplingRatiosAreRejected()
    {
        await Assert.That(static () => new OccasionallyConnectedActivities(samplingRatio: -1D)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => new OccasionallyConnectedActivities(samplingRatio: double.NaN)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => new OccasionallyConnectedActivities(samplingRatio: double.PositiveInfinity)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => new OccasionallyConnectedActivities(samplingRatio: double.NegativeInfinity)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => new OccasionallyConnectedActivities(samplingRatio: 2D)).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies unknown bounded activity values are rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UndefinedActivityNameIsRejected()
    {
        using var recorder = new OccasionallyConnectedActivities();

        await Assert.That(() => recorder.Start((OccasionallyConnectedActivityName)(-1))).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies injected sampling receives a stable parent trace identifier and has deterministic results.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task InjectedSamplerUsesTheCurrentTraceIdentifierDeterministically()
    {
        using var listener = CreateListener();
        var observedTraceIds = new List<ActivityTraceId>();
        using var recorder = new OccasionallyConnectedActivities(
            samplingRatio: HalfSamplingRatio,
            sampler: (traceId, _) =>
            {
                observedTraceIds.Add(traceId);
                return traceId.ToHexString().StartsWith("4bf92f", StringComparison.Ordinal);
            });
        using var parent = new Activity(ParentOperationName)
            .SetParentId("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .Start();

        using var first = recorder.Start(OccasionallyConnectedActivityName.SyncReceive);
        using var second = recorder.Start(OccasionallyConnectedActivityName.SyncReceive);

        await Assert.That(first).IsNotNull();
        await Assert.That(second).IsNotNull();
        await Assert.That(observedTraceIds.Count).IsEqualTo(ExpectedSamplerCalls);
        await Assert.That(observedTraceIds.TrueForAll(traceId => traceId == RequireActivity(parent).TraceId)).IsTrue();
    }

    /// <summary>Verifies concurrent starts are safe and each listener callback observes a defined operation.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConcurrentStartsAreSafe()
    {
        var startedOperations = new ConcurrentBag<string>();
        using var listener = CreateListener(startedOperations);
        using var recorder = new OccasionallyConnectedActivities();

        var tasks = new Task<bool>[ConcurrentStartCount];
        for (var index = 0; index < tasks.Length; index++)
        {
            tasks[index] = Task.Run(() => StartPushActivity(recorder));
        }

        var results = await Task.WhenAll(tasks);

        await Assert.That(Array.TrueForAll(results, static result => result)).IsTrue();
        await Assert.That(startedOperations.Count).IsEqualTo(ConcurrentStartCount);
        await Assert.That(Array.TrueForAll(startedOperations.ToArray(), static name => name == "oc.sync.push")).IsTrue();
    }

    /// <summary>Rejects invalid configuration before publishing a source to process listeners.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task InvalidConfigurationDoesNotCreateAnActivitySource()
    {
        var published = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source =>
            {
                if (source.Name != OccasionallyConnectedActivities.DiagnosticSourceName)
                {
                    return false;
                }

                published++;
                return false;
            },
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.None,
        };
        ActivitySource.AddActivityListener(listener);
        published = 0;
        await Assert.That(static () => new OccasionallyConnectedActivities(samplingRatio: double.NaN))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(published).IsEqualTo(0);
    }

    /// <summary>Root activities need a nonconstant sampling seed when no ambient parent exists.</summary>
    /// <param name="hierarchicalParent">Whether the parent uses a hierarchical identifier.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RootActivityDoesNotSampleTheDefaultTraceIdentifier(bool hierarchicalParent)
    {
        var previous = Activity.Current;
        ActivityTraceId sampled = default;
        using var recorder = new OccasionallyConnectedActivities(sampler: (trace, _) =>
        {
            sampled = trace;
            return false;
        });
        try
        {
            Activity.Current = null;
            using var parent = hierarchicalParent
                ? new Activity(ParentOperationName).SetIdFormat(ActivityIdFormat.Hierarchical).Start()
                : null;
            using var activity = recorder.Start(OccasionallyConnectedActivityName.ContextStart);
            await Assert.That(sampled).IsNotEqualTo(default(ActivityTraceId));
        }
        finally
        {
            Activity.Current = previous;
        }
    }

    /// <summary>Requires the listener-observed activity without nullability suppression.</summary>
    /// <param name="activity">The activity returned by the recorder.</param>
    /// <returns>The started activity.</returns>
    /// <exception cref="InvalidOperationException">The listener did not observe an activity.</exception>
    private static Activity RequireActivity(Activity? activity) =>
        activity ?? throw new InvalidOperationException("The listener did not observe a started activity.");

    /// <summary>Starts and disposes a push activity for the concurrent test.</summary>
    /// <param name="recorder">The recorder under test.</param>
    /// <returns><see langword="true"/> when an activity was created.</returns>
    private static bool StartPushActivity(OccasionallyConnectedActivities recorder)
    {
        using var activity = recorder.Start(OccasionallyConnectedActivityName.SyncPush);
        return activity is not null;
    }

    /// <summary>Creates a listener that samples activities from the recorder source.</summary>
    /// <param name="startedOperations">An optional sink for operations observed by listener callbacks.</param>
    /// <returns>The registered listener.</returns>
    private static ActivityListener CreateListener(ConcurrentBag<string>? startedOperations = null)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == OccasionallyConnectedActivities.DiagnosticSourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };

        if (startedOperations is not null)
        {
            listener.ActivityStarted = activity => startedOperations.Add(activity.OperationName);
        }

        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
