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
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <content>
/// Subscribe tests for combining operators.
/// </content>
public partial class CombiningOperatorTests
{
    /// <summary>
    /// Tests that SubscribeAsync with Action{T} overload works correctly.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithActionOverload_ThenReceivesItems()
    {
        var items = new List<int>();
        var source = ObservableAsync.Range(1, 3);

        await using var sub = await source.SubscribeAsync(items.Add, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= 3,
            TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Tests SubscribeAsync sync overload with error and completion handlers where both are null.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSyncOverloadWithNullHandlers_ThenOnlyOnNextCalled()
    {
        var items = new List<int>();
        var source = ObservableAsync.Range(1, 3);

        await using var sub = await source.SubscribeAsync(
            (Action<int>)items.Add,
            onErrorResume: null,
            onCompleted: null,
            CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= 3,
            TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo([SampleValue1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Tests SubscribeAsync sync overload with a non-null onErrorResume handler invoked.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSyncOverloadWithErrorHandler_ThenErrorHandlerCalled()
    {
        Exception? receivedError = null;
        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("test error"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source.SubscribeAsync(
            (Action<int>)(_ => { }),
            (Action<Exception>?)(ex => receivedError = ex),
            null,
            CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => receivedError is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(receivedError).IsNotNull();
        await Assert.That(receivedError!.Message).IsEqualTo("test error");
    }

    /// <summary>
    /// Tests SubscribeAsync sync overload with a non-null onCompleted handler invoked.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSyncOverloadWithCompletedHandler_ThenCompletedCalled()
    {
        Result? completionResult = null;

        await using var sub = await ObservableAsync.Return(1).SubscribeAsync(
            (Action<int>)(_ => { }),
            (Action<Exception>?)null,
            (Action<Result>?)(result => completionResult = result),
            CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }
}
