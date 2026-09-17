// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests.Internals;

/// <summary>Tests for <see cref="SwitchToSignal{T}.SwitchToCoordinator"/>, which tracks the generation of the current inner sequence.</summary>
public class SwitchToCoordinatorTests
{
    /// <summary>The generation assigned to the first inner sequence.</summary>
    private const long FirstGeneration = 1;

    /// <summary>The generation assigned to the second inner sequence.</summary>
    private const long SecondGeneration = 2;

    /// <summary>Verifies that a completion or a finished subscribe from a superseded inner leaves the current inner in charge.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSupersededInnerCompletesOrFinishesSubscribing_ThenCurrentInnerKeepsTheSwitchOpen()
    {
        CompletionCountingObserver observer = new();
        SwitchToSignal<int>.SwitchToCoordinator coordinator = new(observer);
        var first = Signal.Create<int>();
        var second = Signal.Create<int>();
        await coordinator.AcceptOuterValueAsync(first.Values);
        await coordinator.AcceptOuterValueAsync(second.Values);
        await coordinator.AcceptOuterCompletionAsync(Result.Success);

        await coordinator.AcceptInnerCompletionAsync(FirstGeneration, Result.Success);
        await Assert.That(observer.Completions).IsEqualTo(0);

        StrongBox<bool> staleDisposed = new();
        var staleInner = SignalAsync.Create<int>((_, _) => new(DisposableAsync.Create(staleDisposed, static state =>
        {
            state.Value = true;
            return default;
        })));
        await coordinator.SubscribeReplacementInnerAsync(staleInner, null, FirstGeneration);
        await Assert.That(staleDisposed.Value).IsTrue();
        await Assert.That(observer.Completions).IsEqualTo(0);

        await coordinator.AcceptInnerCompletionAsync(SecondGeneration, Result.Success);
        await Assert.That(observer.Completions).IsEqualTo(1);

        await first.DisposeAsync();
        await second.DisposeAsync();
    }

    /// <summary>Observer that counts terminal notifications.</summary>
    private sealed class CompletionCountingObserver : IObserverAsync<int>
    {
        /// <summary>Gets the number of completions received.</summary>
        public int Completions { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result)
        {
            Completions++;
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
