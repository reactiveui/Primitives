// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.RegularExpressions;

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Direct coverage for the trivial <c>OnError</c> forwarders on a cluster of small
/// synchronous operators. Each method is a one-liner that hands a source error straight to
/// the downstream observer; the existing happy-path tests never exercised the error branch.</summary>
public class SyncOperatorErrorForwardingTests
{
    /// <summary>Synthetic error message used by every forwarder test.</summary>
    private const string ForwardedMessage = "forwarded";

    /// <summary>Verifies <c>TrySelectObservable</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTrySelectSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(ForwardedMessage);

        using var sub = subject.TrySelect(static x => (int?)x)
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>WhereTrueObservable</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereTrueSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<bool>();
        Exception? caught = null;
        var expected = new InvalidOperationException(ForwardedMessage);

        using var sub = subject.WhereTrue().Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>WhereFalseObservable</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereFalseSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<bool>();
        Exception? caught = null;
        var expected = new InvalidOperationException(ForwardedMessage);

        using var sub = subject.WhereFalse().Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>WhereIsNotNullObservable</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereIsNotNullSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<string?>();
        Exception? caught = null;
        var expected = new InvalidOperationException(ForwardedMessage);

        using var sub = subject.WhereIsNotNull().Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>SkipWhileNullObservable</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileNullSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<string>();
        Exception? caught = null;
        var expected = new InvalidOperationException(ForwardedMessage);

        using var sub = subject.SkipWhileNull().Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>FilterRegexObservable</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFilterRegexSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<string>();
        Exception? caught = null;
        var expected = new InvalidOperationException(ForwardedMessage);

        using var sub = subject.Filter(new Regex("x", RegexOptions.None, TimeSpan.FromSeconds(1)))
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>SelectConstantObservable</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectConstantSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(ForwardedMessage);

        using var sub = subject.SelectConstant("constant")
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
