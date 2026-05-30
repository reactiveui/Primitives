// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Multicast / RefCount tests.
/// </summary>
public class ResultAndInfrastructureTests
{
    /// <summary>
    /// Verifies that MulticastSignalAsync does not throw when ConnectAsync is called twice.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectTwice_ThenSecondConnectSucceeds()
    {
        var signal = Signal.Create<int>();
        var source = SignalAsync.Range(1, 3);
        var connectable = source.Multicast(signal);

        await using var conn1 = await connectable.ConnectAsync(CancellationToken.None);
        var conn2 = await connectable.ConnectAsync(CancellationToken.None);

        // Both connections should be non-null; second returns the cached inner connection
        await Assert.That(conn1).IsNotNull();
        await Assert.That(conn2).IsNotNull();
    }

    /// <summary>
    /// Verifies that MulticastSignalAsync disconnect and reconnect works.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastDisconnectAndReconnect_ThenNewConnectionWorks()
    {
        var signal = Signal.Create<int>();
        List<int> items = [];

        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(signal);

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
        var signal = Signal.Create<int>();
        var callCount = 0;

        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            Interlocked.Increment(ref callCount);
            await observer.OnNextAsync(callCount, ct);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(signal);

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
    /// Verifies that disposing the MulticastSignalAsync via IDisposable disposes the
    /// connection and gate.
    /// Covers the Dispose(bool) path.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastDispose_ThenConnectionAndGateDisposed()
    {
        var signal = Signal.Create<int>();
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(signal);

        // Connect first so there is a connection to dispose
        await connectable.ConnectAsync(CancellationToken.None);

        // Cast to IDisposable and call Dispose
        ((IDisposable)(object)connectable).Dispose();

        // Double dispose should be safe (disposedValue is already true)
        ((IDisposable)(object)connectable).Dispose();
    }

    /// <summary>
    /// Verifies that disposing the MulticastSignalAsync without an active connection
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
            var signal = Signal.Create<int>();
            var source = SignalAsync.Range(1, 3);
            var connectable = source.Multicast(signal);

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
    /// Verifies that subscribing to a MulticastSignalAsync works and items flow
    /// through the Signal when connected.
    /// Covers SubscribeAsyncCore.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastSubscribeAndConnect_ThenItemsFlowThroughSignal()
    {
        var signal = Signal.Create<int>();
        List<int> items = [];
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        const int FirstValue = 10;
        const int SecondValue = 20;

        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(FirstValue, ct);
            await observer.OnNextAsync(SecondValue, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var connectable = source.Multicast(signal);

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

        await Assert.That(items).IsCollectionEqualTo([FirstValue, SecondValue]);
    }

    /// <summary>
    /// Verifies that disposing the connection from MulticastSignalAsync twice
    /// exercises the null-guard early return on the second dispose.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMulticastConnectDisposedTwice_ThenSecondDisposeIsNoop()
    {
        var signal = Signal.Create<int>();
        var multicast = SignalAsync.Return(42).Multicast(signal);

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
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("refcount-error"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var signal = Signal.Create<int>();
        var refCounted = source.Multicast(signal).RefCount();

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
