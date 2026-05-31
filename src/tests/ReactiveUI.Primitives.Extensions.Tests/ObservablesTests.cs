// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for the <see cref="Observables"/> factory methods.</summary>
public class ObservablesTests
{
    /// <summary>Verifies <see cref="Observables.Return{T}"/> emits the single value and completes on subscribe.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReturn_ThenEmitsValueAndCompletes()
    {
        const int Value = 42;
        var values = new List<int>();
        var completed = false;

        using var sub = Observables.Return(Value).Subscribe(values.Add, () => completed = true);

        await Assert.That(values).IsCollectionEqualTo([Value]);
        await Assert.That(completed).IsTrue();
    }
}
