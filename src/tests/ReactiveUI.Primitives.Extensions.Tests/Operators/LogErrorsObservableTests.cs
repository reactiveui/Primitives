// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref="LogErrorsObservable{T}"/> — verifies the logger is tapped on
/// the error path, never on the success path, and that the null-observer subscribe guard fires.</summary>
public class LogErrorsObservableTests
{
    /// <summary>Sentinel value flowing through the success path.</summary>
    private const int Sentinel = 1;

    /// <summary>Verifies the logger is invoked with the source error and the error is forwarded downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLogErrorsSourceErrors_ThenLoggerInvokedAndErrorForwarded()
    {
        var subject = new Subject<int>();
        Exception? logged = null;
        Exception? caught = null;
        var values = new List<int>();
        var expected = new InvalidOperationException("logged");

        using var sub = subject.LogErrors(ex => logged = ex).Subscribe(values.Add, ex => caught = ex);

        subject.OnNext(Sentinel);
        subject.OnError(expected);

        await Assert.That(values).IsCollectionEqualTo([Sentinel]);
        await Assert.That(logged).IsSameReferenceAs(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies completion is forwarded without invoking the logger.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLogErrorsSourceCompletes_ThenLoggerNotInvoked()
    {
        var subject = new Subject<int>();
        var logged = 0;
        var completed = false;

        using var sub = subject.LogErrors(_ => logged++).Subscribe(static _ => { }, () => completed = true);

        subject.OnCompleted();

        await Assert.That(logged).IsEqualTo(0);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies subscribing with a null observer throws.</summary>
    [Test]
    public void WhenLogErrorsObserverNull_ThenSubscribeThrows()
    {
        var observable = new LogErrorsObservable<int>(new Subject<int>(), static _ => { });

        Assert.Throws<ArgumentNullException>(() => observable.Subscribe(null!));
    }
}
