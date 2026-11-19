// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for <see cref="MapKeepSignal{TSource, TResult}"/>, <see cref="MapKeepNotNullSignal{TSource, TResult}"/>,
/// <see cref="MapKeepTypeSignal{TSource, TResult}"/>, and <see cref="MapCastSignal{TSource, TResult}"/>.
/// </summary>
public sealed class MapKeepSignalTests
{
    /// <summary>Second input value.</summary>
    private const int Two = 2;

    /// <summary>Third input value.</summary>
    private const int Three = 3;

    /// <summary>Fourth input value.</summary>
    private const int Four = 4;

    /// <summary>Map followed by Keep fuses into MapKeepSignal on live sources.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Map_FollowedByKeep_FusesIntoMapKeepSignal()
    {
        using Signal<int> live = new();
        var pipeline = live.Map(static x => x + 1).Keep(static x => (x & 1) == 0);
        await Assert.That(pipeline).IsTypeOf<MapKeepSignal<int, int>>();

        List<int> values = [];
        using var sub = pipeline.Subscribe(values.Add);
        live.OnNext(1);
        live.OnNext(Two);
        live.OnNext(Three);

        await Assert.That(values.SequenceEqual([Two, Four])).IsTrue();
    }

    /// <summary>Map followed by KeepNotNull fuses into MapKeepNotNullSignal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Map_FollowedByKeepNotNull_FusesIntoMapKeepNotNullSignal()
    {
        var pipeline = Signal.Sequence(1, Four)
            .Map(static x => (x & 1) == 0 ? $"v{x}" : null)
            .KeepNotNull();

        await Assert.That(pipeline).IsTypeOf<MapKeepNotNullSignal<int, string>>();
        List<string> values = [];
        _ = pipeline.Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(["v2", "v4"])).IsTrue();
    }

    /// <summary>Map followed by KeepType fuses into MapKeepTypeSignal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Map_FollowedByKeepType_FusesIntoMapKeepTypeSignal()
    {
        var pipeline = Signal.Sequence(1, Four)
            .Map(static x => (x & 1) == 0 ? (object)$"s{x}" : x)
            .KeepType<string>();

        await Assert.That(pipeline).IsTypeOf<MapKeepTypeSignal<int, string>>();
        List<string> values = [];
        _ = pipeline.Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(["s2", "s4"])).IsTrue();
    }

    /// <summary>Map followed by CastTo fuses into MapCastSignal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Map_FollowedByCastTo_FusesIntoMapCastSignal()
    {
        var pipeline = Signal.Sequence(1, Two)
            .Map(static x => (object)$"c{x}")
            .CastTo<string>();

        await Assert.That(pipeline).IsTypeOf<MapCastSignal<int, string>>();
        List<string> values = [];
        _ = pipeline.Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(["c1", "c2"])).IsTrue();
    }

    /// <summary>Verifies live-source witness paths, terminal forwarding, and current-thread requirements across MapKeep and MapKeepNotNull signals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task MapKeepAndMapKeepNotNullSignals_LiveSources_ForwardNotifications()
    {
        using Signal<int> keepLive = new();
        MapKeepSignal<int, int> mapKeep = new(keepLive, static x => x + 1, static x => (x & 1) == 0);
        await Assert.That(mapKeep.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        RecordingWitness<int> keepDoneObs = new();
        using (mapKeep.Subscribe(keepDoneObs))
        {
            keepLive.OnCompleted();
        }

        await Assert.That(keepDoneObs.Completed).IsEqualTo(1);

        using Signal<int> keepErrLive = new();
        RecordingWitness<int> keepErrObs = new();
        using var keepErrSub = new MapKeepSignal<int, int>(keepErrLive, static x => x, static _ => true).Subscribe(keepErrObs);
        InvalidOperationException boom = new("boom");
        keepErrLive.OnError(boom);
        await Assert.That(keepErrObs.Errors.Single()).IsSameReferenceAs(boom);

        using Signal<int> notNullLive = new();
        MapKeepNotNullSignal<int, string> mapNotNull = new(notNullLive, static x => (x & 1) == 0 ? $"v{x}" : null);
        await Assert.That(mapNotNull.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        RecordingWitness<string> notNullObs = new();
        using (mapNotNull.Subscribe(notNullObs))
        {
            notNullLive.OnNext(1);
            notNullLive.OnNext(Two);
            notNullLive.OnCompleted();
        }

        await Assert.That(notNullObs.Values.SequenceEqual(["v2"])).IsTrue();
        await Assert.That(notNullObs.Completed).IsEqualTo(1);

        using Signal<int> notNullErrLive = new();
        RecordingWitness<string> notNullErrObs = new();
        using var notNullErrSub = new MapKeepNotNullSignal<int, string>(notNullErrLive, static _ => "x").Subscribe(notNullErrObs);
        notNullErrLive.OnError(boom);
        await Assert.That(notNullErrObs.Errors.Single()).IsSameReferenceAs(boom);
    }

    /// <summary>Verifies live-source witness paths, terminal forwarding, and current-thread requirements across MapKeepType and MapCast signals.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task MapKeepTypeAndMapCastSignals_LiveSources_ForwardNotifications()
    {
        InvalidOperationException boom = new("boom");
        using Signal<int> typeLive = new();
        MapKeepTypeSignal<int, string> mapType = new(typeLive, static x => (x & 1) == 0 ? (object)$"s{x}" : x);
        await Assert.That(mapType.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        RecordingWitness<string> typeObs = new();
        using (mapType.Subscribe(typeObs))
        {
            typeLive.OnNext(1);
            typeLive.OnNext(Two);
            typeLive.OnCompleted();
        }

        await Assert.That(typeObs.Values.SequenceEqual(["s2"])).IsTrue();
        await Assert.That(typeObs.Completed).IsEqualTo(1);

        using Signal<int> typeErrLive = new();
        RecordingWitness<string> typeErrObs = new();
        using var typeErrSub = new MapKeepTypeSignal<int, string>(typeErrLive, static _ => "x").Subscribe(typeErrObs);
        typeErrLive.OnError(boom);
        await Assert.That(typeErrObs.Errors.Single()).IsSameReferenceAs(boom);

        using Signal<int> castLive = new();
        MapCastSignal<int, string> mapCast = new(castLive, static x => $"c{x}");
        await Assert.That(mapCast.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        RecordingWitness<string> castObs = new();
        using (mapCast.Subscribe(castObs))
        {
            castLive.OnNext(1);
            castLive.OnCompleted();
        }

        await Assert.That(castObs.Values.SequenceEqual(["c1"])).IsTrue();
        await Assert.That(castObs.Completed).IsEqualTo(1);

        using Signal<int> castErrLive = new();
        RecordingWitness<string> castErrObs = new();
        using var castErrSub = new MapCastSignal<int, string>(castErrLive, static x => $"c{x}").Subscribe(castErrObs);
        castErrLive.OnError(boom);
        await Assert.That(castErrObs.Errors.Single()).IsSameReferenceAs(boom);
    }
}
