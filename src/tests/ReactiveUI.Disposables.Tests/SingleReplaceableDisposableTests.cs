// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests replacement ownership when an observed slot changes.</summary>
public class SingleReplaceableDisposableTests
{
    /// <summary>The callback count after disposal and a later rejected assignment.</summary>
    private const int DisposeAndAssignmentCallbacks = 2;

    /// <summary>A stale replacement leaves both the current and incoming resources untouched.</summary>
    /// <param name="initiallyEmpty">True when the original slot is empty.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TryCreate_StaleObservation_RetriesWithoutReleasingUnownedValues(bool initiallyEmpty)
    {
        CountingDisposable initial = new();
        CountingDisposable competing = new();
        CountingDisposable incoming = new();
        IDisposable? observed = initiallyEmpty ? null : initial;
        SingleReplaceableDisposable slot = initiallyEmpty ? new() : new(initial);
        slot.Create(competing);

        await Assert.That(slot.TryCreate(observed, incoming)).IsFalse();
        await Assert.That(initial.DisposeCount).IsEqualTo(initiallyEmpty ? 0 : 1);
        await Assert.That(competing.DisposeCount).IsEqualTo(0);
        await Assert.That(incoming.DisposeCount).IsEqualTo(0);

        await Assert.That(slot.TryCreate(competing, incoming)).IsTrue();
        await Assert.That(competing.DisposeCount).IsEqualTo(1);
        await Assert.That(incoming.DisposeCount).IsEqualTo(0);
        slot.Dispose();
        await Assert.That(incoming.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposal between observation and assignment rejects publication and releases the later assignment.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TryCreate_DisposedAfterObservation_DoesNotPublishIncomingValue()
    {
        CountingDisposable initial = new();
        CountingDisposable incoming = new();
        var callbacks = 0;
        SingleReplaceableDisposable slot = new(initial, () => callbacks++);
        slot.Dispose();

        await Assert.That(slot.TryCreate(initial, incoming)).IsFalse();
        await Assert.That(initial.DisposeCount).IsEqualTo(1);
        await Assert.That(incoming.DisposeCount).IsEqualTo(0);

        slot.Create(incoming);

        await Assert.That(slot.IsDisposed).IsTrue();
        await Assert.That(incoming.DisposeCount).IsEqualTo(1);
        await Assert.That(callbacks).IsEqualTo(DisposeAndAssignmentCallbacks);
    }

    /// <summary>Counts every disposal invocation.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of disposal invocations.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
