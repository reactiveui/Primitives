// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Tests CatchIgnore without error action.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CatchIgnore_OnError_ReturnsEmpty()
    {
        using Subject<int> subject = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.CatchIgnore().Subscribe(
            results.Add,
            _ => { },
            () => completed = true);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnError(new InvalidOperationException());
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed).IsTrue();
        }
    }

    /// <summary>Tests CatchIgnore with error action.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CatchIgnore_WithErrorAction_CallsActionAndReturnsEmpty()
    {
        using Subject<int> subject = new();
        List<int> results = [];
        var errorCaught = false;
        var completed = false;
        using var sub = subject.CatchIgnore<int, InvalidOperationException>(ex => errorCaught = true).Subscribe(
            results.Add,
            _ => { },
            () => completed = true);
        subject.OnNext(1);
        subject.OnError(new InvalidOperationException());
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1]);
            await Assert.That(errorCaught).IsTrue();
            await Assert.That(completed).IsTrue();
        }
    }

    /// <summary>Tests CatchAndReturn with fallback value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CatchAndReturn_OnError_ReturnsFallback()
    {
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.CatchAndReturn(99).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnError(new InvalidOperationException());
        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue99]);
    }

    /// <summary>Tests LogErrors invokes the logger when the source faults.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task LogErrors_WhenSourceErrors_InvokesLogger()
    {
        Exception? logged = null;
        Exception? observed = null;
        using Subject<int> subject = new();
        using var sub = subject.LogErrors(ex => logged = ex).Subscribe(
            _ => { },
            ex => observed = ex);
        InvalidOperationException exception = new("boom");
        subject.OnError(exception);
        using (Assert.Multiple())
        {
            await Assert.That(logged).IsSameReferenceAs(exception);
            await Assert.That(observed).IsSameReferenceAs(exception);
        }
    }

    /// <summary>Tests CatchAndReturn with factory recovers from a specific exception type.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturnWithFactory_ThenRecoverFromException()
    {
        Subject<int> subject = new();
        List<int> results = [];
        subject.CatchAndReturn<int, InvalidOperationException>(ex => -1).Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnError(new InvalidOperationException("boom"));
        await Assert.That(results).IsCollectionEqualTo([1, -1]);
    }
}
