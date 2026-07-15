// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the shared guard observer used by the scheduled factory signals.</summary>
public sealed class GuardedWitnessTests
{
    /// <summary>Value forwarded through the guard.</summary>
    private const int One = 1;

    /// <summary>Second value used to verify forwarding continues after a value.</summary>
    private const int Two = 2;

    /// <summary>Verifies the constructor rejects null arguments.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullArguments()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            GuardedWitness<int> invalid = new(new RecordingWitness<int>(), null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            GuardedWitness<int> invalid = new(null!, EmptyDisposable.Instance);
            GC.KeepAlive(invalid);
        });

        await Task.CompletedTask;
    }

    /// <summary>Verifies values forward without releasing the cancel resource.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForwardsValuesWithoutDisposing()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable cancel = new();
        GuardedWitness<int> witness = new(observer, cancel);

        witness.OnNext(One);
        witness.OnNext(Two);

        await Assert.That(observer.Values.SequenceEqual([One, Two])).IsTrue();
        await Assert.That(cancel.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies a throwing downstream OnNext releases the cancel resource and rethrows.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnNextThrowDisposesAndRethrows()
    {
        RecordingDisposable cancel = new();
        GuardedWitness<int> witness = new(new ThrowingWitness<int>(true), cancel);

        _ = Assert.Throws<InvalidOperationException>(() => witness.OnNext(One));

        await Assert.That(cancel.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies errors forward and the cancel resource is released even when downstream throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorForwardsThenDisposes()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable cancel = new();
        GuardedWitness<int> witness = new(observer, cancel);
        InvalidOperationException error = new("guarded");

        witness.OnError(error);

        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(cancel.DisposeCount).IsEqualTo(1);

        RecordingDisposable throwingCancel = new();
        GuardedWitness<int> throwing = new(new ThrowingWitness<int>(throwOnError: true), throwingCancel);
        _ = Assert.Throws<InvalidOperationException>(() => throwing.OnError(error));
        await Assert.That(throwingCancel.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies completion forwards and the cancel resource is released even when downstream throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnCompletedForwardsThenDisposes()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable cancel = new();
        GuardedWitness<int> witness = new(observer, cancel);

        witness.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(cancel.DisposeCount).IsEqualTo(1);

        RecordingDisposable throwingCancel = new();
        GuardedWitness<int> throwing = new(new ThrowingWitness<int>(throwOnCompleted: true), throwingCancel);
        _ = Assert.Throws<InvalidOperationException>(throwing.OnCompleted);
        await Assert.That(throwingCancel.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies disposal is idempotent.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeIsIdempotent()
    {
        RecordingDisposable cancel = new();
        GuardedWitness<int> witness = new(new RecordingWitness<int>(), cancel);

        witness.Dispose();
        witness.Dispose();

        await Assert.That(cancel.DisposeCount).IsEqualTo(1);
    }
}
