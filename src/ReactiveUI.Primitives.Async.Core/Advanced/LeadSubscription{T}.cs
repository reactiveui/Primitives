// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Subscription that emits leading values and then relays the source sequence.</summary>
/// <typeparam name="T">The element type.</typeparam>
public sealed class LeadSubscription<T> : IAsyncDisposable
{
    /// <summary>Protects disposal state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="LeadSubscription{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="values">The leading values.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="subscriptionToken">The subscribe-time cancellation token.</param>
    public LeadSubscription(
        IObservableAsync<T> source,
        IEnumerable<T> values,
        IObserverAsync<T> observer,
        CancellationToken subscriptionToken)
    {
        Source = source;
        Values = values;
        Observer = observer;
        Cancellation = CancellationTokenSource.CreateLinkedTokenSource(subscriptionToken);
        Pipeline = Task.CompletedTask;
    }

    /// <summary>Gets the source sequence.</summary>
    private IObservableAsync<T> Source { get; }

    /// <summary>Gets the leading values.</summary>
    private IEnumerable<T> Values { get; }

    /// <summary>Gets the downstream observer.</summary>
    private IObserverAsync<T> Observer { get; }

    /// <summary>Gets the upstream subscription slot.</summary>
    private SingleAssignmentDisposableAsync SourceSubscription { get; } = new();

    /// <summary>Gets the subscription cancellation source.</summary>
    private CancellationTokenSource Cancellation { get; }

    /// <summary>Gets the reentrancy marker used to avoid self-joining the driver task.</summary>
    private AsyncLocal<bool> Reentrant { get; } = new();

    /// <summary>Gets or sets the driver task.</summary>
    private Task Pipeline { get; set; }

    /// <summary>Gets or sets a value indicating whether disposal has already run.</summary>
    private bool Disposed { get; set; }

    /// <summary>Starts the lead pipeline.</summary>
    public void Start() => Pipeline = RunAsync();

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (Disposed)
            {
                return;
            }

            Disposed = true;
        }

        await SourceSubscription.DisposeAsync().ConfigureAwait(false);
        await Cancellation.CancelAsync().ConfigureAwait(false);
        if (!Reentrant.Value)
        {
            await Pipeline.ConfigureAwait(false);
        }

        Cancellation.Dispose();
    }

    /// <summary>Runs the prefix emission and source subscription pipeline.</summary>
    /// <returns>A task that completes when the source has been subscribed or the prefix pipeline fails.</returns>
    private async Task RunAsync()
    {
        Reentrant.Value = true;
        try
        {
            if (!await TryEmitLeadingValuesAsync().ConfigureAwait(false))
            {
                return;
            }

            var subscription = await Source.SubscribeAsync(Observer.Wrap(), Cancellation.Token).ConfigureAwait(false);
            await SourceSubscription.SetDisposableAsync(subscription).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cooperative subscription cancellation.
        }
        catch (Exception e)
        {
            await ReportFailureAsync(e).ConfigureAwait(false);
        }
    }

    /// <summary>Emits the configured leading values.</summary>
    /// <returns><see langword="true"/> if all values were emitted; otherwise <see langword="false"/>.</returns>
    private async ValueTask<bool> TryEmitLeadingValuesAsync()
    {
        foreach (var value in Values)
        {
            if (Cancellation.IsCancellationRequested)
            {
                return false;
            }

            await Observer.OnNextAsync(value, Cancellation.Token).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>Reports a terminal failure to the downstream observer.</summary>
    /// <param name="error">The failure.</param>
    /// <returns>A task representing the asynchronous notification.</returns>
    private async ValueTask ReportFailureAsync(Exception error)
    {
        try
        {
            await Observer.OnCompletedAsync(Result.Failure(error)).ConfigureAwait(false);
        }
        catch (Exception escalated)
        {
            UnhandledExceptionHandler.ReportUnhandledException(escalated);
        }
    }
}
