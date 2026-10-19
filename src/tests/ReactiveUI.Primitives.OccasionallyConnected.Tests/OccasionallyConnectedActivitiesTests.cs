// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Diagnostics;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedActivities"/>.</summary>
[NotInParallel]
public sealed partial class OccasionallyConnectedActivitiesTests
{
    /// <summary>The number of concurrent activity starts to perform.</summary>
    private const int ConcurrentStartCount = 64;

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

    /// <summary>Verifies unknown bounded activity values are rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UndefinedActivityNameIsRejected()
    {
        using var recorder = new OccasionallyConnectedActivities();

        await Assert.That(() => recorder.Start((OccasionallyConnectedActivityName)(-1))).ThrowsExactly<ArgumentOutOfRangeException>();
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

    /// <summary>Leaves sampling decisions entirely to the host listener.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HostListenerCanRejectEveryActivity()
    {
        var result = await RunActivityProbeAsync("reject");
        await Assert.That(result).IsEqualTo("True|1");
    }

    /// <summary>Skips activity creation when the host has no listener.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task NoListenerDoesNotCreateAnActivityOrChangeTheParent()
    {
        var result = await RunActivityProbeAsync("none");
        await Assert.That(result).IsEqualTo("True|True");
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
