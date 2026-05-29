// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Threading.Channels;
using ReactiveUI.Primitives.Async;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>ForEachAsync / ToAsyncEnumerable / Wrap parameter-validation and failure-propagation
/// tests, split out of <c>TerminalOperatorTests.cs</c> so the main file stays under the 1000-line
/// limit imposed by S104.</summary>
public partial class TerminalOperatorTests
{
    /// <summary>Tests ForEachAsync with null sync action throws ArgumentNullException.</summary>
    [Test]
    public void WhenForEachAsyncWithNullSyncAction_ThenThrowsArgumentNullException() =>
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await ObservableAsync.Return(1).ForEachAsync((Action<int>)null!));

    /// <summary>Tests ForEachAsync with null async action throws ArgumentNullException.</summary>
    [Test]
    public void WhenForEachAsyncWithNullAsyncAction_ThenThrowsArgumentNullException() =>
        Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await ObservableAsync.Return(1).ForEachAsync(null!));

    /// <summary>Tests async ForEachAsync propagates source failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsyncSourceFails_ThenThrows()
    {
        var error = new InvalidOperationException("test");
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ObservableAsync.Throw<int>(error).ForEachAsync((_, _) => default));
    }

    /// <summary>Tests sync ForEachAsync propagates source failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsyncSyncOverloadSourceFails_ThenThrows()
    {
        var error = new InvalidOperationException("test");
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ObservableAsync.Throw<int>(error).ForEachAsync(_ => { }));
    }

    /// <summary>Tests ToAsyncEnumerable propagates source failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToAsyncEnumerableSourceFails_ThenThrows()
    {
        var error = new InvalidOperationException("enum-error");
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in ObservableAsync.Throw<int>(error)
                               .ToAsyncEnumerable(() => Channel.CreateUnbounded<int>()))
            {
                _ = item;
            }
        });
    }

    /// <summary>Tests Wrap with a null observer throws ArgumentNullException.</summary>
    [Test]
    public void WhenWrapWithNullObserver_ThenThrowsArgumentNullException() =>
        Assert.Throws<ArgumentNullException>(() => ObservableAsync.Wrap<int>(null!));

    /// <summary>Verifies the async-callback <c>ForEachAsync</c> overload throws when the
    /// callback delegate is null.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsyncCallbackNull_ThenThrowsArgumentNullException() =>
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await ObservableAsync.Return(1).ForEachAsync(null!, CancellationToken.None));
}
