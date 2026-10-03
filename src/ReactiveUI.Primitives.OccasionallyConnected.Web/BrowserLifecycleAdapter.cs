// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Channels;
using Microsoft.JSInterop;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;

namespace ReactiveUI.Primitives.OccasionallyConnected.Web;

/// <summary>Composes browser lifecycle hints with an existing occasionally connected context.</summary>
/// <remarks>
/// Hidden, frozen, or departed pages stop the context. Visible pages restart it. Network availability requests sync
/// but never establishes Online. Events coalesce into one pending state. The caller owns the context and store.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("{ConnectivityHint}, Suspended = {IsSuspended}")]
public sealed class BrowserLifecycleAdapter : IAsyncDisposable
{
    /// <summary>The packaged JS module path.</summary>
    private const string ModulePath = "./_content/ReactiveUI.Primitives.OccasionallyConnected.Web/browserLifecycle.js";

    /// <summary>The browser runtime.</summary>
    private readonly IJSRuntime _runtime;

    /// <summary>Identifies this listener set even if a cancelled invocation loses its JS reference.</summary>
    private readonly string _registrationId = Guid.NewGuid().ToString("N");

    /// <summary>The caller-owned context.</summary>
    private readonly IOccasionallyConnectedContext _context;

    /// <summary>Serializes listener setup and teardown.</summary>
    private readonly SemaphoreSlim _initialization = new(1, 1);

    /// <summary>Cancels operations when the adapter is disposed.</summary>
    private readonly CancellationTokenSource _lifetime = new();

    /// <summary>Protects cancellation and idle state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The single pending browser snapshot.</summary>
    private readonly Channel<(bool Available, bool Suspended)> _updates =
        Channel.CreateBounded<(bool Available, bool Suspended)>(
            new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });

    /// <summary>The serialized lifecycle worker.</summary>
    private readonly Task _worker;

    /// <summary>Shares disposal completion with repeated callers.</summary>
    private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes when the pending state has been applied.</summary>
    private TaskCompletionSource _idle = CompletedSource();

    /// <summary>Cancels the current lifecycle operation.</summary>
    private Func<Task>? _cancelActive;

    /// <summary>The completion of cancellation callbacks for the current operation.</summary>
    private Task? _activeCancellation;

    /// <summary>The managed reference held by JS listeners.</summary>
    private DotNetObjectReference<BrowserLifecycleAdapter>? _reference;

    /// <summary>The imported module.</summary>
    private IJSObjectReference? _module;

    /// <summary>The JS listener registration.</summary>
    private IJSObjectReference? _listeners;

    /// <summary>The most recent lifecycle failure.</summary>
    private Exception? _lastError;

    /// <summary>Indicates that teardown has started.</summary>
    private int _disposeStarted;

    /// <summary>The latest network hint.</summary>
    private int _hint;

    /// <summary>The latest suspension hint.</summary>
    private int _suspended = 1;

    /// <summary>Indicates that context startup completed.</summary>
    private bool _running;

    /// <summary>Initializes a new instance of the <see cref="BrowserLifecycleAdapter"/> class.</summary>
    /// <param name="runtime">The browser JS runtime.</param>
    /// <param name="context">The context whose start and stop lifecycle this adapter controls.</param>
    public BrowserLifecycleAdapter(IJSRuntime runtime, IOccasionallyConnectedContext context)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        ArgumentNullException.ThrowIfNull(context);
        _runtime = runtime;
        _context = context;
        _worker = ProcessAsync();
    }

    /// <summary>Gets the latest network hint. This is never an Online claim.</summary>
    public BrowserConnectivityHint ConnectivityHint => (BrowserConnectivityHint)Volatile.Read(ref _hint);

    /// <summary>Gets whether the latest browser state requests suspension.</summary>
    public bool IsSuspended => Volatile.Read(ref _suspended) != 0;

    /// <summary>Gets the last lifecycle error, cleared after a successful update.</summary>
    /// <remarks>No automatic retries occur. A later browser event may retry the lifecycle operation.</remarks>
    public Exception? LastError => Volatile.Read(ref _lastError);

    /// <summary>Creates the shared IndexedDB implementation without adding storage guarantees.</summary>
    /// <param name="runtime">The browser JS runtime.</param>
    /// <returns>An IndexedDB store owned by the caller or its context.</returns>
    public static IndexedDbLocalStoreAdapter CreateLocalStore(IJSRuntime runtime) => new(runtime);

    /// <summary>Imports the static asset and installs browser listeners once.</summary>
    /// <param name="cancellationToken">Cancels listener initialization.</param>
    /// <returns>The initialization task. Context lifecycle work proceeds asynchronously.</returns>
    /// <remarks>Call after interactive rendering; JS interop is unavailable during prerendering.</remarks>
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        await _initialization.WaitAsync(linked.Token).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeStarted) != 0, this);
            if (_listeners is not null)
            {
                return;
            }

            _module ??= await _runtime.InvokeAsync<IJSObjectReference>("import", linked.Token, ModulePath).ConfigureAwait(false);
            _reference ??= DotNetObjectReference.Create(this);
            _listeners = await _module.InvokeAsync<IJSObjectReference>(
                "observe",
                linked.Token,
                _registrationId,
                _reference).ConfigureAwait(false);
        }
        catch (JSException error)
        {
            Volatile.Write(ref _lastError, error);
            throw;
        }
        catch (OperationCanceledException)
        {
            await RemoveListenersAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _ = _initialization.Release();
        }
    }

    /// <summary>Accepts a browser snapshot through JS interop.</summary>
    /// <param name="networkAvailable">The navigator network hint.</param>
    /// <param name="suspended">Whether visibility or page lifecycle requests suspension.</param>
    /// <returns>The cancellation notification task; lifecycle work runs on one bounded lane.</returns>
    [JSInvokable]
    public Task OnBrowserStateChangedAsync(bool networkAvailable, bool suspended)
    {
        var cancellation = Task.CompletedTask;
        lock (_gate)
        {
            if (Volatile.Read(ref _disposeStarted) != 0)
            {
                return Task.CompletedTask;
            }

            Volatile.Write(ref _hint, (int)(networkAvailable
                ? BrowserConnectivityHint.PossiblyAvailable
                : BrowserConnectivityHint.Unavailable));
            Volatile.Write(ref _suspended, suspended ? 1 : 0);
            if (suspended)
            {
                if (_cancelActive is not null)
                {
                    _activeCancellation ??= _cancelActive();
                }

                cancellation = _activeCancellation ?? Task.CompletedTask;
            }

            if (_idle.Task.IsCompleted)
            {
                _idle = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _ = _updates.Writer.TryWrite((networkAvailable, suspended));
        }

        return cancellation;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) != 0)
        {
            await _disposed.Task.ConfigureAwait(false);
            return;
        }

        try
        {
            await DisposeResourcesAsync().ConfigureAwait(false);
            _ = _disposed.TrySetResult();
        }
        catch (Exception error)
        {
            _ = _disposed.TrySetException(error);
            throw;
        }
    }

    /// <summary>Waits for the current bounded batch to finish for deterministic tests.</summary>
    /// <returns>The drain task.</returns>
    internal async Task DrainAsync()
    {
        Task idle;
        lock (_gate)
        {
            idle = _idle.Task;
        }

        await idle.ConfigureAwait(false);
    }

    /// <summary>Creates an already-completed idle marker.</summary>
    /// <returns>The completed source.</returns>
    private static TaskCompletionSource CompletedSource()
    {
        var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        source.SetResult();
        return source;
    }

    /// <summary>Applies browser snapshots in one serialized lane.</summary>
    /// <returns>The worker task.</returns>
    private async Task ProcessAsync()
    {
        await foreach (var state in _updates.Reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
        {
            var operation = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
            bool stale;
            lock (_gate)
            {
                _cancelActive = operation.CancelAsync;
                stale = IsSuspended && !state.Suspended;
            }

            if (stale)
            {
                await operation.CancelAsync().ConfigureAwait(false);
            }

            try
            {
                await ApplyAsync(state.Available, state.Suspended, operation.Token).ConfigureAwait(false);
                Volatile.Write(ref _lastError, null);
            }
            catch (OperationCanceledException) when (operation.IsCancellationRequested)
            {
                // A newer suspension or disposal replaces this work.
            }
            catch (Exception error)
            {
                Volatile.Write(ref _lastError, error);
            }
            finally
            {
                await FinishOperationAsync(operation).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Drains cancellation callbacks before releasing an operation's token source.</summary>
    /// <param name="operation">The completed lifecycle operation's token source.</param>
    /// <returns>The cleanup task.</returns>
    private async Task FinishOperationAsync(CancellationTokenSource operation)
    {
        Task? cancellation;
        lock (_gate)
        {
            cancellation = _activeCancellation;
            _cancelActive = null;
            _activeCancellation = null;
        }

        try
        {
            if (cancellation is not null)
            {
                await cancellation.ConfigureAwait(false);
            }
        }
        catch (Exception error)
        {
            Volatile.Write(ref _lastError, error);
        }
        finally
        {
            operation.Dispose();
        }

        lock (_gate)
        {
            if (!_updates.Reader.TryPeek(out _))
            {
                _ = _idle.TrySetResult();
            }
        }
    }

    /// <summary>Applies one lifecycle snapshot without claiming server connectivity.</summary>
    /// <param name="available">The browser network hint.</param>
    /// <param name="suspended">The browser suspension hint.</param>
    /// <param name="cancellationToken">Cancels superseded active work.</param>
    /// <returns>The lifecycle task.</returns>
    private async ValueTask ApplyAsync(bool available, bool suspended, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (suspended)
        {
            _running = false;
            await _context.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!_running)
        {
            await _context.StartAsync(cancellationToken).ConfigureAwait(false);
            _running = true;
        }

        if (!available)
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();
        await _context.SyncEngine.TriggerSyncAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes listeners, drains the worker, and stops the caller-owned context.</summary>
    /// <returns>The cleanup task.</returns>
    private async Task DisposeResourcesAsync()
    {
        try
        {
            await _lifetime.CancelAsync().ConfigureAwait(false);
        }
        catch (AggregateException error)
        {
            Volatile.Write(ref _lastError, error);
        }

        _ = _updates.Writer.TryComplete();
        await _worker.ConfigureAwait(false);
        await _initialization.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            try
            {
                await RemoveListenersAsync().ConfigureAwait(false);
            }
            finally
            {
                if (_listeners is not null)
                {
                    try
                    {
                        await _listeners.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (JSDisconnectedException)
                    {
                        // No JS reference remains when the circuit has disconnected.
                    }
                }
            }
        }
        finally
        {
            _reference?.Dispose();
            try
            {
                if (_module is not null)
                {
                    try
                    {
                        await _module.DisposeAsync().ConfigureAwait(false);
                    }
                    catch (JSDisconnectedException)
                    {
                        // Module references cannot be released through a disconnected circuit.
                    }
                }
            }
            finally
            {
                _ = _initialization.Release();
                _initialization.Dispose();
                _lifetime.Dispose();
                await _context.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Removes the keyed listener set even when initialization lost its returned reference.</summary>
    /// <returns>The listener removal task.</returns>
    private async ValueTask RemoveListenersAsync()
    {
        if (_module is null)
        {
            return;
        }

        try
        {
            await _module.InvokeVoidAsync("unobserve", _registrationId).ConfigureAwait(false);
        }
        catch (JSDisconnectedException)
        {
            // A departed circuit cannot retain a live .NET listener registration.
        }
    }
}
