// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpRequestGate"/>.</summary>
public sealed class HttpRequestGateTests
{
    /// <summary>The single admitted request capacity.</summary>
    private const int SingleCapacity = 1;

    /// <summary>Verifies admission observes caller cancellation before changing gate state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EnterAsyncThrowsWhenCancellationAlreadyRequested()
    {
        var gate = new HttpRequestGate(SingleCapacity);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.That(async () => await gate.EnterAsync(cancellation.Token)).ThrowsExactly<OperationCanceledException>();
    }

    /// <summary>Verifies a disposed gate rejects new admissions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EnterAsyncThrowsAfterGateDisposal()
    {
        var gate = new HttpRequestGate(SingleCapacity);
        await gate.DisposeAsync();

        await Assert.That(async () => await gate.EnterAsync(CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposal waits for the active lease and duplicate lease disposal is harmless.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncWaitsForActiveLeaseAndLeaseDisposeIsIdempotent()
    {
        var gate = new HttpRequestGate(SingleCapacity);
        var lease = await gate.EnterAsync(CancellationToken.None);

        var dispose = gate.DisposeAsync().AsTask();
        lease.Dispose();
        lease.Dispose();

        await dispose;
        await Assert.That(dispose.IsCompletedSuccessfully).IsTrue();
    }
}
