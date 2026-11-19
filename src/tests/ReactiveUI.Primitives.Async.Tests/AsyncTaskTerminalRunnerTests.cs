// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="AsyncTaskTerminalRunner"/>.</summary>
public sealed class AsyncTaskTerminalRunnerTests
{
    /// <summary>Second sample value.</summary>
    private const int Two = 2;

    /// <summary>Third sample value.</summary>
    private const int Three = 3;

    /// <summary>Expected accumulated sum.</summary>
    private const int Six = 6;

    /// <summary>
    /// Verifies <see cref="AsyncTaskTerminalRunner.RunToResultAsync{TSource, TWitness, TResult}"/> and
    /// <see cref="AsyncTaskTerminalRunner.RunToCompletionAsync{TSource, TWitness, TResult}"/> subscribe, return or complete,
    /// and dispose upstream handles.
    /// </summary>
    /// <param name="cancellationToken">Token supplied by TUnit.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RunToResultAndRunToCompletion_ReturnAndDisposeUpstream(CancellationToken cancellationToken)
    {
        DisposalCounter disposals = new();
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnNextAsync(Two, ct);
            await observer.OnNextAsync(Three, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Create(
                disposals,
                static state =>
                {
                    state.Count++;
                    return default;
                });
        });

        SumTerminalWitness sumWitness = new(cancellationToken);
        var sum = await AsyncTaskTerminalRunner.RunToResultAsync(source, sumWitness, sumWitness.Completion, cancellationToken);
        await Assert.That(sum).IsEqualTo(Six);
        await Assert.That(disposals.Count).IsEqualTo(1);

        SumTerminalWitness completionWitness = new(cancellationToken);
        await AsyncTaskTerminalRunner.RunToCompletionAsync(source, completionWitness, completionWitness.Completion, cancellationToken);
        await Assert.That(disposals.Count).IsEqualTo(Two);
    }

    /// <summary>Verifies <see cref="AsyncTaskTerminalRunner.CompleteEmptySequence{TWitness, TResult}"/> produces appropriate exceptions for empty, unmatched, and faulted sequences.</summary>
    /// <param name="cancellationToken">Token supplied by TUnit.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompleteEmptySequence_ProducesExpectedFaults(CancellationToken cancellationToken)
    {
        SumTerminalWitness emptyWitness = new(cancellationToken);
        await AsyncTaskTerminalRunner.CompleteEmptySequence(emptyWitness.Completion, Result.Success, hasPredicate: false, emptyWitness);
        var emptyError = await Assert.That(async () => await emptyWitness.Completion.AwaitResultAsync(emptyWitness)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(emptyError!.Message).IsEqualTo("Sequence contains no elements.");

        SumTerminalWitness unmatchedWitness = new(cancellationToken);
        await AsyncTaskTerminalRunner.CompleteEmptySequence(unmatchedWitness.Completion, Result.Success, hasPredicate: true, unmatchedWitness);
        var unmatchedError = await Assert.That(async () => await unmatchedWitness.Completion.AwaitResultAsync(unmatchedWitness)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(unmatchedError!.Message).IsEqualTo("Sequence contains no matching elements.");

        InvalidOperationException upstreamFault = new("upstream fault");
        SumTerminalWitness faultedWitness = new(cancellationToken);
        await AsyncTaskTerminalRunner.CompleteEmptySequence(faultedWitness.Completion, Result.Failure(upstreamFault), hasPredicate: false, faultedWitness);
        var propagated = await Assert.That(async () => await faultedWitness.Completion.AwaitResultAsync(faultedWitness)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(propagated).IsSameReferenceAs(upstreamFault);
    }

    /// <summary>Verifies <see cref="AsyncTaskTerminalRunner.CountMatchingInt32{T}"/> and <see cref="AsyncTaskTerminalRunner.CountMatchingInt64{T}"/> filter and guard overflow.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CountMatching_IncrementsAcceptedValuesAndChecksOverflow()
    {
        var count32 = 0;
        await AsyncTaskTerminalRunner.CountMatchingInt32(1, null, ref count32);
        await AsyncTaskTerminalRunner.CountMatchingInt32(Two, static v => v > 1, ref count32);
        await AsyncTaskTerminalRunner.CountMatchingInt32(1, static v => v > 1, ref count32);
        await Assert.That(count32).IsEqualTo(Two);

        var count64 = 0L;
        await AsyncTaskTerminalRunner.CountMatchingInt64(1, null, ref count64);
        await AsyncTaskTerminalRunner.CountMatchingInt64(Three, static v => v > Two, ref count64);
        await AsyncTaskTerminalRunner.CountMatchingInt64(Two, static v => v > Two, ref count64);
        await Assert.That(count64).IsEqualTo(Two);

        await Assert.That(static async () =>
        {
            var max32 = int.MaxValue;
            await AsyncTaskTerminalRunner.CountMatchingInt32(1, null, ref max32);
        }).Throws<OverflowException>();
    }

    /// <summary>Tracks upstream subscription disposals without closure capture.</summary>
    private sealed class DisposalCounter
    {
        /// <summary>Gets or sets the disposal count.</summary>
        public int Count { get; set; }
    }

    /// <summary>Minimal terminal witness double summing observed integers.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    private sealed class SumTerminalWitness(CancellationToken cancellationToken) : IWitnessAsync<int>
    {
        /// <summary>Running sum.</summary>
        private int _sum;

        /// <summary>Backing witness state.</summary>
        private WitnessAsyncState _witness;

        /// <summary>Gets the completion source.</summary>
        public TaskResultCompletionSource<int> Completion { get; } = new(cancellationToken);

        /// <inheritdoc/>
        public ref WitnessAsyncState Witness => ref _witness;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) =>
            WitnessAsync.OnNextAsync(this, value, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
            WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<int>.OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            _sum += value;
            return default;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            Completion.SetExceptionAndDisposeAsync(error, this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask IWitnessAsync<int>.OnCompletedAsyncCore(Result result) =>
            Completion.CompleteAndDisposeAsync(result, _sum, this);
    }
}
