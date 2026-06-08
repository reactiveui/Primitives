// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Terminal-notification coverage for <c>ScanWithInitial</c> — source error, source completion, and post-terminal value ignore.</summary>
public partial class ScanWithInitialTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Initial accumulator value.</summary>
    private const int TerminalInitial = 0;

    /// <summary>Verifies that <c>OnError</c> is forwarded after the initial value has been emitted.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = subject.ScanWithInitial(TerminalInitial, static (acc, x) => acc + x)
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>OnCompleted</c> is forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanSourceCompletes_ThenForwardsCompletion()
    {
        var subject = new Subject<int>();
        var completed = false;

        using var sub = subject.ScanWithInitial(TerminalInitial, static (acc, x) => acc + x)
            .Subscribe(static _ => { }, () => completed = true);

        subject.OnCompleted();

        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that values arriving after <c>OnError</c> are ignored.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanValueAfterError_ThenIgnored()
    {
        const int IgnoredValue = 5;
        var subject = new Subject<int>();
        var results = new List<int>();
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = subject.ScanWithInitial(TerminalInitial, static (acc, x) => acc + x)
            .Subscribe(results.Add, static _ => { });

        subject.OnError(expected);
        subject.OnNext(IgnoredValue);

        await Assert.That(results).IsCollectionEqualTo([TerminalInitial]);
    }

    /// <summary>Verifies that values arriving after <c>OnCompleted</c> are ignored.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanValueAfterCompleted_ThenIgnored()
    {
        const int IgnoredValue = 5;
        var subject = new Subject<int>();
        var results = new List<int>();

        using var sub = subject.ScanWithInitial(TerminalInitial, static (acc, x) => acc + x)
            .Subscribe(results.Add);

        subject.OnCompleted();
        subject.OnNext(IgnoredValue);

        await Assert.That(results).IsCollectionEqualTo([TerminalInitial]);
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the sink has marked itself terminated are silently dropped via the
    /// <c>_done</c> guard.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterTerminated_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        Exception? caught = null;
        var completedCount = 0;

        using var sub = source.ScanWithInitial(TerminalInitial, static (acc, x) => acc + x)
            .Subscribe(values.Add, ex => caught = ex, () => completedCount++);

        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();

        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsCollectionEqualTo([TerminalInitial]);
        await Assert.That(caught).IsNull();
    }
}
