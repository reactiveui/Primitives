// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the fused <see cref = "LinqExtensions.SwitchMap{TIn, TOut}"/> projection operator.</summary>
public class SwitchMapTests
{
    /// <summary>The value ten.</summary>
    private const int Ten = 10;

    /// <summary>The value eleven, produced by a superseded inner and therefore ignored.</summary>
    private const int Eleven = 11;

    /// <summary>The value twenty.</summary>
    private const int Twenty = 20;

    /// <summary>The value thirty.</summary>
    private const int Thirty = 30;

    /// <summary>The expected single-occurrence count.</summary>
    private const int Once = 1;

    /// <summary>The first outer selector key.</summary>
    private const string KeyA = "a";

    /// <summary>The second outer selector key.</summary>
    private const string KeyB = "b";

    /// <summary>Shared error message.</summary>
    private const string Boom = "boom";

    /// <summary>Expected forwarded values after switching inner sources.</summary>
    private static readonly int[] _tenThenTwenty = [Ten, Twenty];

    /// <summary>Expected forwarded values when a null switches onto its own inner.</summary>
    private static readonly int[] _tenThenThirty = [Ten, Thirty];

    /// <summary>Expected single forwarded value.</summary>
    private static readonly int[] _tenOnly = [Ten];

    /// <summary>Verifies that the latest inner is mirrored and a superseded inner is ignored.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapMirrorsLatestInnerAndIgnoresStaleInner()
    {
        Signal<string> outer = new();
        Signal<int> inner1 = new();
        Signal<int> inner2 = new();
        List<int> values = [];
        _ = outer.SwitchMap(key => key == KeyA ? inner1 : inner2).Subscribe(values.Add);
        outer.OnNext(KeyA);
        inner1.OnNext(Ten);
        outer.OnNext(KeyB);
        inner1.OnNext(Eleven);
        inner2.OnNext(Twenty);
        await Assert.That(values.SequenceEqual(_tenThenTwenty)).IsTrue();
    }

    /// <summary>
    /// Verifies that a null source value switches onto whatever the selector returns for it, which detaches the
    /// previous inner rather than leaving it attached.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapSwitchesOnNullSourceValue()
    {
        Signal<string?> outer = new();
        Signal<int> inner = new();
        Signal<int> fallback = new();
        List<int> values = [];
        _ = outer.SwitchMap(key => key is null ? fallback : inner).Subscribe(values.Add);
        outer.OnNext(KeyA);
        inner.OnNext(Ten);
        outer.OnNext(null);
        inner.OnNext(Eleven);
        fallback.OnNext(Thirty);
        await Assert.That(values.SequenceEqual(_tenThenThirty)).IsTrue();
    }

    /// <summary>Verifies that completion waits for both the outer and the active inner.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapCompletesAfterOuterAndInner()
    {
        Signal<string> outer = new();
        Signal<int> inner = new();
        var completed = 0;
        _ = outer.SwitchMap(_ => inner).Subscribe(
            static _ => { },
            static ex => throw ex,
            () => completed++);
        outer.OnNext(KeyA);
        outer.OnCompleted();
        await Assert.That(completed).IsEqualTo(0);
        inner.OnCompleted();
        await Assert.That(completed).IsEqualTo(Once);
    }

    /// <summary>Verifies that an outer that completes without ever producing a value completes downstream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapCompletesWhenOuterCompletesWithoutInner()
    {
        Signal<string> outer = new();
        var completed = 0;
        _ = outer.SwitchMap(static _ => new Signal<int>()).Subscribe(
            static _ => { },
            static ex => throw ex,
            () => completed++);
        outer.OnCompleted();
        await Assert.That(completed).IsEqualTo(Once);
    }

    /// <summary>Verifies that a selector exception terminates the sequence with that error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapForwardsSelectorError()
    {
        Signal<string> outer = new();
        Exception? error = null;
        _ = outer.SwitchMap<string, int>(static _ => throw new InvalidOperationException(Boom)).Subscribe(
            static _ => { },
            ex => error = ex,
            static () => { });
        outer.OnNext(KeyA);
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that an outer error terminates the sequence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapForwardsOuterError()
    {
        Signal<string> outer = new();
        Exception? error = null;
        _ = outer.SwitchMap(static _ => new Signal<int>()).Subscribe(
            static _ => { },
            ex => error = ex,
            static () => { });
        outer.OnError(new InvalidOperationException(Boom));
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that an error from the active inner terminates the sequence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapForwardsInnerError()
    {
        Signal<string> outer = new();
        Signal<int> inner = new();
        Exception? error = null;
        _ = outer.SwitchMap(_ => inner).Subscribe(
            static _ => { },
            ex => error = ex,
            static () => { });
        outer.OnNext(KeyA);
        inner.OnError(new InvalidOperationException(Boom));
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that disposing the subscription stops forwarding inner values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapStopsForwardingAfterDispose()
    {
        Signal<string> outer = new();
        Signal<int> inner = new();
        List<int> values = [];
        var subscription = outer.SwitchMap(_ => inner).Subscribe(values.Add);
        outer.OnNext(KeyA);
        inner.OnNext(Ten);
        subscription.Dispose();
        inner.OnNext(Eleven);
        await Assert.That(values.SequenceEqual(_tenOnly)).IsTrue();
    }

    /// <summary>Verifies that the sink rejects a null source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapSignalRejectsNullSource() =>
        await Assert.That(static () => new SwitchMapSignal<string, int>(null!, static _ => new Signal<int>()))
            .Throws<ArgumentNullException>();

    /// <summary>Verifies that the sink rejects a null selector.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapSignalRejectsNullSelector() =>
        await Assert.That(static () => new SwitchMapSignal<string, int>(new Signal<string>(), null!))
            .Throws<ArgumentNullException>();

    /// <summary>Verifies that the sink rejects a null observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchMapSignalRejectsNullObserver() =>
        await Assert.That(static () =>
                new SwitchMapSignal<string, int>(new Signal<string>(), static _ => new Signal<int>()).Subscribe(null!))
            .Throws<ArgumentNullException>();
}
