// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Channels;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>ForEachAsync / ToAsyncEnumerable / Wrap parameter-validation and failure-propagation
/// tests, split out of <c>TerminalOperatorTests.cs</c> so the main file stays under the 1000-line
/// limit imposed by S104.</summary>
public partial class TerminalOperatorTests
{
    /// <summary>Tests ForEachAsync with null sync action throws ArgumentNullException.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenForEachAsyncWithNullSyncAction_ThenThrowsArgumentNullException() => await Assert
        .That(async () => await SignalAsync.Return(1).ForEachAsync((Action<int>)null!))
        .ThrowsExactly<ArgumentNullException>();

    /// <summary>Tests ForEachAsync with null async action throws ArgumentNullException.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenForEachAsyncWithNullAsyncAction_ThenThrowsArgumentNullException() => await Assert
        .That(async () => await SignalAsync.Return(1).ForEachAsync(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Tests async ForEachAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsyncSourceFails_ThenThrows()
    {
        InvalidOperationException error = new("test");
        await Assert.That(async () => await SignalAsync.Throw<int>(error).ForEachAsync((_, _) => default))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests sync ForEachAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsyncSyncOverloadSourceFails_ThenThrows()
    {
        InvalidOperationException error = new("test");
        await Assert.That(async () => await SignalAsync.Throw<int>(error).ForEachAsync(_ => { }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests ToAsyncEnumerable propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToAsyncEnumerableSourceFails_ThenThrows()
    {
        InvalidOperationException error = new("enum-error");
        await Assert.That(async () =>
        {
            await foreach (var item in SignalAsync.Throw<int>(error)
                               .ToAsyncEnumerable(() => Channel.CreateUnbounded<int>()))
            {
                _ = item;
            }
        }).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests Wrap with a null observer throws ArgumentNullException.</summary>
    [Test]
    public void WhenWrapWithNullObserver_ThenThrowsArgumentNullException() =>
        Assert.Throws<ArgumentNullException>(() => SignalAsyncExtensions.Wrap<int>(null!));

    /// <summary>Verifies the async-callback <c>ForEachAsync</c> overload throws when the callback delegate is null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsyncCallbackNull_ThenThrowsArgumentNullException() => await Assert
        .That(async () => await SignalAsync.Return(1).ForEachAsync(null!, CancellationToken.None))
        .ThrowsExactly<ArgumentNullException>();
}
