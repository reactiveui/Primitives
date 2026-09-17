// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the observer that resumes an awaiting continuation.</summary>
public class AwaitWitnessTests
{
    /// <summary>An error resumes the continuation inline when context capture is disabled.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnError_WithoutCapturedContext_InvokesContinuationInline()
    {
        var calls = 0;
        AwaitWitness<int> witness = new(() => calls++, false);

        witness.OnError(new InvalidOperationException("source"));

        await Assert.That(calls).IsEqualTo(1);
    }
}
