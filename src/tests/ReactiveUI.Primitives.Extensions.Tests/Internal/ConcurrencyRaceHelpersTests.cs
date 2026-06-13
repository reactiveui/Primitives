// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Direct RxVoid tests for <see cref="ConcurrencyRaceHelpers"/> — both race-claim
/// primitives are pure functions over their inputs and every branch is exercised here.</summary>
public class ConcurrencyRaceHelpersTests
{
    /// <summary>Sentinel for the "not yet claimed" state in the tests.</summary>
    private const int Open = 0;

    /// <summary>Sentinel for the "claimed" state in the tests.</summary>
    private const int Claimed = 1;

    /// <summary>Verifies <see cref="ConcurrencyRaceHelpers.TryClaim"/> succeeds when the state is open and transitions it to the claimed sentinel.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTryClaimOpen_ThenReturnsTrueAndTransitions()
    {
        var state = Open;

        var claimed = ConcurrencyRaceHelpers.TryClaim(ref state, Open, Claimed);

        await Assert.That(claimed).IsTrue();
        await Assert.That(state).IsEqualTo(Claimed);
    }

    /// <summary>Verifies <see cref="ConcurrencyRaceHelpers.TryClaim"/> returns false when the state is already claimed and does not mutate it further.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTryClaimAlreadyClaimed_ThenReturnsFalse()
    {
        var state = Claimed;

        var claimed = ConcurrencyRaceHelpers.TryClaim(ref state, Open, Claimed);

        await Assert.That(claimed).IsFalse();
        await Assert.That(state).IsEqualTo(Claimed);
    }

    /// <summary>Verifies <see cref="ConcurrencyRaceHelpers.TryCancelAsync"/> returns <see langword="true"/> when called on an open CTS and the cancellation goes through.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTryCancelOpenCts_ThenReturnsTrueAndTokenCancels()
    {
        CancellationTokenSource cts = new();

        var succeeded = await ConcurrencyRaceHelpers.TryCancelAsync(cts);

        await Assert.That(succeeded).IsTrue();
        await Assert.That(cts.IsCancellationRequested).IsTrue();
    }

    /// <summary>Verifies cancelling a disposed CTS returns false and swallows the dispose exception.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTryCancelDisposedCts_ThenReturnsFalseAndSwallowsObjectDisposed()
    {
        CancellationTokenSource cts = new();
        cts.Dispose();

        var succeeded = await ConcurrencyRaceHelpers.TryCancelAsync(cts);

        await Assert.That(succeeded).IsFalse();
    }
}
