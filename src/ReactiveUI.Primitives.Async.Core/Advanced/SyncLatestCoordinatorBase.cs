// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Base class for the arity-specific <c>CombineLatestN</c> coordinators, owning the
/// <see cref="SyncLatestLifecycle{TResult}"/> wiring, the values-lock, the source-subscribe loop,
/// the error-resume forwarder and disposal.
/// </summary>
/// <typeparam name="TResult">The downstream element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SyncLatestCoordinatorBase: SourceCount = {Lifecycle.Subscriptions.Length}, HasDisposed = {Lifecycle.HasDisposed}")]
public abstract class SyncLatestCoordinatorBase<TResult> : IAsyncDisposable
{
    /// <summary>Initializes a new instance of the <see cref="SyncLatestCoordinatorBase{TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="sourceCount">The number of upstream sources (e.g. 2 for arity-2).</param>
    protected SyncLatestCoordinatorBase(IObserverAsync<TResult> observer, int sourceCount) =>
        Lifecycle = new(observer, sourceCount);

    /// <summary>Gets the shared subscription lifecycle (gate / dispose CTS / external link / forwarders).</summary>
    internal SyncLatestLifecycle<TResult> Lifecycle { get; }

    /// <summary>Gets the lock guarding the derived coordinator's latest-value slots.</summary>
    internal Lock ValuesLock { get; } = new();

    /// <summary>Subscribes to every source observable via <see cref="SubscribeAtAsync"/>.</summary>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>A task representing the asynchronous subscribe operation.</returns>
    public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
    {
        var subs = Lifecycle.Subscriptions;
        for (var i = 0; i < subs.Length; i++)
        {
            subs[i] = await SubscribeAtAsync(i, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await Lifecycle.DisposeAsync().ConfigureAwait(false);

        GC.SuppressFinalize(this);
    }

    /// <summary>Relays an upstream error to the downstream observer.</summary>
    /// <param name="error">The error to forward.</param>
    /// <param name="cancellationToken">Ignored; the lifecycle uses its own dispose token.</param>
    /// <returns>A ValueTask representing the asynchronous forward.</returns>
    internal ValueTask RelaySourceErrorAsync(Exception error, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return Lifecycle.OnErrorResumeAsync(error);
    }

    /// <summary>Projects the latest values once every source has emitted. Called after updating a slot under ValuesLock.</summary>
    /// <returns>A ValueTask representing the asynchronous emit.</returns>
    internal abstract ValueTask EmitLatestAsync();

    /// <summary>Subscribes to a single source by 0-based index, dispatching to the derived coordinator's typed slot.</summary>
    /// <param name="index">0-based source index.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>The subscription disposable for the source at <paramref name="index"/>.</returns>
    protected abstract ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken);
}
