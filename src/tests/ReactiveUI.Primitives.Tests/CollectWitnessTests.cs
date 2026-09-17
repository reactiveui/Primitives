// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests argument validation of <see cref="CollectWitness{T}"/>.</summary>
public sealed class CollectWitnessTests
{
    /// <summary>A null observer is rejected by the timed constructor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullObserver_ThrowsArgumentNull() =>
        await Assert.That(static () => new CollectWitness<int>(null!, TimeSpan.Zero, Sequencer.Immediate))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>A null sequencer is rejected by the timed constructor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructor_NullSequencer_ThrowsArgumentNull() =>
        await Assert.That(static () => new CollectWitness<int>(new IgnoringObserver(), TimeSpan.Zero, null!))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>Observer that ignores every notification.</summary>
    private sealed class IgnoringObserver : IObserver<IList<int>>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(IList<int> value)
        {
        }
    }
}
