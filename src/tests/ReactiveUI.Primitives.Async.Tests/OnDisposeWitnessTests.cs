// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="OnDisposeWitness{T}"/>.</summary>
public sealed class OnDisposeWitnessTests
{
    /// <summary>Verifies the synchronous cleanup runs once however many times the witness is disposed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsync_CalledTwice_RunsSyncCleanupOnce()
    {
        var calls = 0;
        OnDisposeWitness<int> witness = new(new CallbackWitnessAsync<int>(static (_, _) => default), () => calls++, null);

        await witness.DisposeAsync();
        await witness.DisposeAsync();

        await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>Verifies the asynchronous cleanup runs when no synchronous cleanup is given, and no cleanup is also accepted.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsync_AsyncOrNoCleanup_RunsWhatIsGiven()
    {
        var calls = 0;
        OnDisposeWitness<int> asyncCleanup = new(
            new CallbackWitnessAsync<int>(static (_, _) => default),
            null,
            async () =>
            {
                await Task.Yield();
                calls++;
            });
        OnDisposeWitness<int> noCleanup = new(new CallbackWitnessAsync<int>(static (_, _) => default), null, null);

        await asyncCleanup.DisposeAsync();
        await noCleanup.DisposeAsync();

        await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>Verifies a throwing cleanup still tears the witness down and then surfaces the failure.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsync_CleanupThrows_RethrowsAfterTeardown()
    {
        InvalidOperationException boom = new("cleanup");
        OnDisposeWitness<int> witness = new(new CallbackWitnessAsync<int>(static (_, _) => default), () => throw boom, null);

        var thrown = await Assert.That(async () => await witness.DisposeAsync()).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(boom);
    }
}
