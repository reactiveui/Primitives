// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="CastSignal{TResult}"/> and <see cref="CastWitness{TResult}"/>.</summary>
public sealed class CastWitnessTests
{
    /// <summary>Second cast value.</summary>
    private const int Two = 2;

    /// <summary>Verifies <see cref="CastSignal{TResult}"/> reductions, current-thread forwarding, and list completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CastSignal_ReductionsCurrentThreadAndValidList_BehaveAsExpected()
    {
        await Assert.That(CastSignal<int>.Create(Signal.Empty<object?>())).IsSameReferenceAs(Signal.Empty<int>());
        await Assert.That(CastSignal<int>.Create(Signal.Silent<object?>())).IsSameReferenceAs(Signal.Silent<int>());

        CastSignal<int> reqTrue = new(new ObjectCurrentThreadStub(true));
        CastSignal<int> reqFalse = new(new ObjectCurrentThreadStub(false));
        CastSignal<int> reqPlain = new(Signal.Empty<object?>());
        await Assert.That(reqTrue.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(reqFalse.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(reqPlain.IsRequiredSubscribeOnCurrentThread()).IsFalse();

        RecordingWitness<int> listObserver = new();
        using var sub = new CastSignal<int>(Signal.FromEnumerable<object?>([1, Two])).Subscribe(listObserver);
        await Assert.That(listObserver.Values.SequenceEqual([1, Two])).IsTrue();
        await Assert.That(listObserver.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref="CastWitness{TResult}"/> routes invalid casts to <c>OnError</c> and suppresses post-terminal notifications.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CastWitness_InvalidCastAndDuplicateTerminals_EnforceSingleTerminal()
    {
        RecordingWitness<int> errObserver = new();
        RecordingDisposable upstream1 = new();
        using CastWitness<int> errWitness = new(errObserver);
        errWitness.SetSubscription(upstream1);

        errWitness.OnNext("not-an-int");
        errWitness.OnNext(1);
        errWitness.OnError(new InvalidOperationException("duplicate"));

        await Assert.That(errObserver.Errors.Single()).IsTypeOf<InvalidCastException>();
        await Assert.That(errObserver.Values).IsEmpty();
        await Assert.That(upstream1.DisposeCount).IsEqualTo(1);

        RecordingWitness<int> doneObserver = new();
        using CastWitness<int> doneWitness = new(doneObserver);
        doneWitness.OnCompleted();
        doneWitness.OnCompleted();
        await Assert.That(doneObserver.Completed).IsEqualTo(1);
    }

    /// <summary>Stub observable implementing <see cref="IRequireCurrentThread{T}"/> for object elements.</summary>
    /// <param name="required">Whether current-thread subscription is required.</param>
    private sealed class ObjectCurrentThreadStub(bool required) : IRequireCurrentThread<object?>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsRequiredSubscribeOnCurrentThread() => required;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(IObserver<object?> observer) => EmptyDisposable.Instance;
    }
}
