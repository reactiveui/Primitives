// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="TapWithSignal{T, TState}"/>.</summary>
public sealed class TapWithSignalTests
{
    /// <summary>Second element value.</summary>
    private const int Two = 2;

    /// <summary>A throwing TapWith callback routes the exception to OnError and disposes upstream.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_CallbackThrows_ForwardsErrorAndDisposesUpstream()
    {
        var disposals = 0;
        IObserver<int>? upstream = null;
        var source = Signal.Create<int>(obs =>
        {
            upstream = obs;
            return new ActionDisposable(() => disposals++);
        });

        RecordingWitness<int> recorder = new();
        _ = source.TapWith(1, static (_, _) => throw new InvalidOperationException("tap")).Subscribe(recorder);
        upstream!.OnNext(1);

        await Assert.That(disposals).IsEqualTo(1);
        await Assert.That(recorder.Errors.Count).IsEqualTo(1);
    }

    /// <summary>FromEnumerable fast path invokes the tap callback for each element.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_FromEnumerableSource_InvokesCallbackForEveryElement()
    {
        var tapped = 0;
        _ = Signal.FromEnumerable([1, Two]).TapWith(1, (_, _) => tapped++).Subscribe(static _ => { });
        await Assert.That(tapped).IsEqualTo(Two);
    }
}
