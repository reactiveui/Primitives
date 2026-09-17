// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests observer detachment from an asynchronous signal.</summary>
public class ObserverHandlerTests
{
    /// <summary>A subscription handle can be disposed repeatedly without detaching another observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_Repeated_PreservesOtherSubscriptions()
    {
        using AsyncSignal<int> signal = new();
        RecordingWitness<int> detached = new();
        RecordingWitness<int> remaining = new();
        var first = signal.Subscribe(detached);
        using var second = signal.Subscribe(remaining);

        first.Dispose();
        first.Dispose();
        signal.OnNext(1);
        signal.OnCompleted();

        await Assert.That(detached.Values.Count).IsEqualTo(0);
        await Assert.That(detached.Completed).IsEqualTo(0);
        await Assert.That(remaining.Values.Single()).IsEqualTo(1);
        await Assert.That(remaining.Completed).IsEqualTo(1);
    }
}
