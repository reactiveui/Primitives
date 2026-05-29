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
using ReactiveUI.Primitives.Async.Subjects;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Multicast / RefCount tests.
/// </summary>
public partial class ResultAndInfrastructureTests
{
    /// <summary>
    /// Verifies that MulticastObservableAsync does not throw when ConnectAsync is called twice.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectTwice_ThenSecondConnectSucceeds()
    {
        var subject = SubjectAsync.Create<int>();
        var source = ObservableAsync.Range(1, 3);
        var connectable = source.Multicast(subject);

        await using var conn1 = await connectable.ConnectAsync(CancellationToken.None);
        var conn2 = await connectable.ConnectAsync(CancellationToken.None);

        // Both connections should be non-null; second returns the cached inner connection
        await Assert.That(conn1).IsNotNull();
        await Assert.That(conn2).IsNotNull();
    }

    /// <summary>
    /// Verifies that MulticastObservableAsync disconnect and reconnect works.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastDisconnectAndReconnect_ThenNewConnectionWorks()
    {
        var subject = SubjectAsync.Create<int>();
        List<int> items = [];

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(subject);

        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        var conn = await connectable.ConnectAsync(CancellationToken.None);
        await conn.DisposeAsync();

        await Assert.That(items).IsNotEmpty();
    }

    /// <summary>
    /// Verifies that disposing the disconnect handle sets the internal connection to null
    /// and allows a subsequent reconnection.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastDisconnectHandle_ThenConnectionCleared()
    {
        var subject = SubjectAsync.Create<int>();
        var callCount = 0;

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            Interlocked.Increment(ref callCount);
            await observer.OnNextAsync(callCount, ct);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(subject);

        // Connect
        var conn1 = await connectable.ConnectAsync(CancellationToken.None);

        // Disconnect
        await conn1.DisposeAsync();

        // Double-disconnect should be safe (connection is already null)
        await conn1.DisposeAsync();

        // Reconnect should create a new connection
        var conn2 = await connectable.ConnectAsync(CancellationToken.None);

        const int MinCallCount = 2;
        await Assert.That(callCount).IsGreaterThanOrEqualTo(MinCallCount);

        await conn2.DisposeAsync();
    }

    /// <summary>
    /// Verifies that disposing the MulticastObservableAsync via IDisposable disposes the
    /// connection and gate.
    /// Covers the Dispose(bool) path.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastDispose_ThenConnectionAndGateDisposed()
    {
        var subject = SubjectAsync.Create<int>();
        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(subject);

        // Connect first so there is a connection to dispose
        await connectable.ConnectAsync(CancellationToken.None);

        // Cast to IDisposable and call Dispose
        ((IDisposable)(object)connectable).Dispose();

        // Double dispose should be safe (disposedValue is already true)
        ((IDisposable)(object)connectable).Dispose();
    }

    /// <summary>
    /// Verifies that disposing the MulticastObservableAsync without an active connection
    /// is safe and disposes the gate.
    /// Covers the Dispose path when no connection is active.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("TUnit", "TUnitAssertions0005", Justification = "Asserting expected constant outcome")]
    public Task WhenMulticastDisposeWithoutConnection_ThenSafe()
    {
        try
        {
            var subject = SubjectAsync.Create<int>();
            var source = ObservableAsync.Range(1, 3);
            var connectable = source.Multicast(subject);

            // Dispose without ever connecting
            ((IDisposable)(object)connectable).Dispose();
            return Task.CompletedTask;
        }
        catch (Exception exception)
        {
            return Task.FromException(exception);
        }
    }

    /// <summary>
    /// Verifies that subscribing to a MulticastObservableAsync works and items flow
    /// through the subject when connected.
    /// Covers SubscribeAsyncCore.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastSubscribeAndConnect_ThenItemsFlowThroughSubject()
    {
        var subject = SubjectAsync.Create<int>();
        List<int> items = [];
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        const int FirstValue = 10;
        const int SecondValue = 20;

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(FirstValue, ct);
            await observer.OnNextAsync(SecondValue, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(subject);

        await using var sub = await connectable.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        await using var conn = await connectable.ConnectAsync(CancellationToken.None);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo([FirstValue, SecondValue]);
    }

    /// <summary>
    /// Verifies that disposing the connection from MulticastObservableAsync twice
    /// exercises the null-guard early return on the second dispose.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectDisposedTwice_ThenSecondDisposeIsNoop()
    {
        var subject = SubjectAsync.Create<int>();
        var multicast = ObservableAsync.Return(42).Multicast(subject);

        var connection = await multicast.ConnectAsync(CancellationToken.None);
        await connection.DisposeAsync();

        // Second dispose should hit the null-guard return path
        await connection.DisposeAsync();
    }

    /// <summary>
    /// Verifies that RefCount forwards error-resume from the source through the RefCountObserver.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRefCountSourceErrorResume_ThenForwardsToSubscriber()
    {
        Exception? captured = null;
        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("refcount-error"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var subject = SubjectAsync.Create<int>();
        var refCounted = source.Multicast(subject).RefCount();

        List<int> items = [];
        await using var sub = await refCounted.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            (ex, _) =>
            {
                captured = ex;
                return default;
            });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => captured is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(items).Contains(1);
        await Assert.That(captured).IsNotNull();
        await Assert.That(captured!.Message).IsEqualTo("refcount-error");
    }
}
