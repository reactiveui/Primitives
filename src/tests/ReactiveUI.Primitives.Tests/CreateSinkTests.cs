// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the shared sink used by the create and safe-create signals.</summary>
public sealed class CreateSinkTests
{
    /// <summary>Value forwarded through the sink.</summary>
    private const int One = 1;

    /// <summary>Value used for late notifications that must be suppressed.</summary>
    private const int Two = 2;

    /// <summary>Verifies values forward until a terminal notification and late notifications are suppressed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForwardsUntilTerminalAndSuppressesLateNotifications()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable cancel = new();
        CreateSink<int> sink = new(observer, disposeOnNextThrow: false);
        sink.SetCancel(cancel);

        sink.OnNext(One);
        sink.OnCompleted();
        sink.OnNext(Two);
        sink.OnError(new InvalidOperationException("late"));
        sink.OnCompleted();

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(cancel.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies errors forward once and release the cancel resource.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorForwardsOnceAndDisposes()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable cancel = new();
        CreateSink<int> sink = new(observer, disposeOnNextThrow: false);
        sink.SetCancel(cancel);
        InvalidOperationException error = new("create-sink");

        sink.OnError(error);
        sink.OnError(new InvalidOperationException("late"));

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(cancel.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies the unsafe variant propagates a throwing OnNext without releasing the subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnsafeVariantPropagatesOnNextThrowWithoutDisposing()
    {
        RecordingDisposable cancel = new();
        CreateSink<int> sink = new(new ThrowingWitness<int>(true), disposeOnNextThrow: false);
        sink.SetCancel(cancel);

        _ = Assert.Throws<InvalidOperationException>(() => sink.OnNext(One));

        await Assert.That(cancel.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies the safe variant releases the subscription when a downstream OnNext throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SafeVariantDisposesOnNextThrow()
    {
        RecordingDisposable cancel = new();
        CreateSink<int> sink = new(new ThrowingWitness<int>(true), disposeOnNextThrow: true);
        sink.SetCancel(cancel);

        _ = Assert.Throws<InvalidOperationException>(() => sink.OnNext(One));

        await Assert.That(cancel.DisposeCount).IsEqualTo(1);
        sink.OnNext(Two);
        await Assert.That(cancel.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies cancel assignment guards: null rejected, duplicates disposed, late assignment disposed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SetCancelGuardsNullDuplicateAndLateAssignment()
    {
        RecordingDisposable first = new();
        RecordingDisposable duplicate = new();
        CreateSink<int> sink = new(new RecordingWitness<int>(), disposeOnNextThrow: false);
        sink.SetCancel(first);

        _ = Assert.Throws<ArgumentNullException>(() => sink.SetCancel(null!));
        sink.SetCancel(duplicate);
        await Assert.That(duplicate.DisposeCount).IsEqualTo(1);
        await Assert.That(first.DisposeCount).IsEqualTo(0);

        RecordingDisposable late = new();
        sink.Dispose();
        sink.SetCancel(late);
        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(first.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies the eager-cancel constructor releases the resource on the terminal path.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EagerCancelConstructorReleasesOnTerminal()
    {
        RecordingWitness<int> observer = new();
        RecordingDisposable cancel = new();
        CreateSink<int> sink = new(observer, cancel, disposeOnNextThrow: true);

        sink.OnNext(One);
        sink.OnCompleted();

        await Assert.That(observer.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(cancel.DisposeCount).IsEqualTo(1);
    }
}
