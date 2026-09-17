// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Tracks asynchronously disposable resources acquired while composing the collaboration server runtime.</summary>
internal sealed class CollaborationServerResourceScope : IAsyncDisposable
{
    /// <summary>The owned resources in acquisition order.</summary>
    private readonly List<IAsyncDisposable> _resources = [];

    /// <summary>The gate that protects ownership transfer and disposal start.</summary>
    private readonly Lock _syncRoot = new();

    /// <summary>The shared completion observed by repeated runtime disposers.</summary>
    private TaskCompletionSource? _disposeCompletion;

    /// <summary>Whether new resources may still be transferred into the scope.</summary>
    private bool _acceptingResources = true;

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        var start = BeginDispose();
        if (start.Completion is not null)
        {
            _ = CompleteDisposeAsync(start.Resources, start.Completion);
        }

        return new(start.SharedTask);
    }

    /// <summary>Transfers ownership of an acquired resource into the scope.</summary>
    /// <typeparam name="TResource">The resource type.</typeparam>
    /// <param name="resource">The acquired resource.</param>
    /// <returns>The tracked resource.</returns>
    internal TResource Track<TResource>(TResource resource)
        where TResource : IAsyncDisposable
    {
        ArgumentNullException.ThrowIfNull(resource);
        lock (_syncRoot)
        {
            ObjectDisposedException.ThrowIf(!_acceptingResources, this);
            _resources.Add(resource);
        }

        return resource;
    }

    /// <summary>Rolls back acquired startup resources while preserving the original startup failure.</summary>
    /// <returns>The asynchronous disposal task.</returns>
    internal ValueTask DisposeSilentlyAsync()
    {
        var start = BeginDispose();
        if (start.Completion is not null)
        {
            _ = CompleteDisposeSilentlyAsync(start.Resources, start.Completion);
        }

        return new(start.SharedTask);
    }

    /// <summary>Rolls back acquired startup resources and faults the returned operation with the original startup failure.</summary>
    /// <typeparam name="T">The startup result type.</typeparam>
    /// <param name="originalException">The original startup failure.</param>
    /// <returns>A task that completes after rollback and then rethrows the original startup failure.</returns>
    internal ValueTask<T> RollbackAsync<T>(Exception originalException)
    {
        ArgumentNullException.ThrowIfNull(originalException);
        var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = CompleteRollbackAsync(DisposeSilentlyAsync(), originalException, completion);
        return new(completion.Task);
    }

    /// <summary>Runs the disposal drain and completes the shared task with the preserved result.</summary>
    /// <param name="resources">The resources captured when disposal started.</param>
    /// <param name="completion">The shared disposal completion.</param>
    /// <returns>The runner task.</returns>
    private static async Task CompleteDisposeAsync(List<IAsyncDisposable> resources, TaskCompletionSource completion)
    {
        try
        {
            await DisposeCoreAsync(resources).ConfigureAwait(false);
            completion.SetResult();
        }
        catch (Exception exception)
        {
            completion.SetException(exception);
        }
    }

    /// <summary>Runs the silent rollback drain and completes the shared task after cleanup attempts finish.</summary>
    /// <param name="resources">The resources captured when disposal started.</param>
    /// <param name="completion">The shared disposal completion.</param>
    /// <returns>The runner task.</returns>
    private static async Task CompleteDisposeSilentlyAsync(List<IAsyncDisposable> resources, TaskCompletionSource completion)
    {
        await DisposeSilentlyCoreAsync(resources).ConfigureAwait(false);
        completion.SetResult();
    }

    /// <summary>Completes startup rollback after observing owned cleanup completion.</summary>
    /// <typeparam name="T">The startup result type.</typeparam>
    /// <param name="rollback">The rollback operation.</param>
    /// <param name="originalException">The original startup failure.</param>
    /// <param name="completion">The startup completion source.</param>
    /// <returns>The rollback driver task.</returns>
    private static async Task CompleteRollbackAsync<T>(
        ValueTask rollback,
        Exception originalException,
        TaskCompletionSource<T> completion)
    {
        try
        {
            await rollback.ConfigureAwait(false);
        }
        catch (Exception cleanupException)
        {
            _ = cleanupException;
        }

        if (originalException is OperationCanceledException cancellation)
        {
            completion.SetCanceled(cancellation.CancellationToken);
            return;
        }

        completion.SetException(originalException);
    }

    /// <summary>Disposes owned resources and preserves the first failure for every caller.</summary>
    /// <param name="resources">The resources captured when disposal started.</param>
    /// <returns>The shared disposal task.</returns>
    private static async Task DisposeCoreAsync(List<IAsyncDisposable> resources)
    {
        Exception? firstException = null;
        for (var index = resources.Count - 1; index >= 0; index--)
        {
            firstException = await CaptureDisposalExceptionAsync(firstException, resources[index].DisposeAsync).ConfigureAwait(false);
        }

        resources.Clear();
        RethrowDisposalFailure(firstException);
    }

    /// <summary>Disposes owned resources during startup rollback without surfacing cleanup failures.</summary>
    /// <param name="resources">The resources captured when rollback started.</param>
    /// <returns>The shared disposal task.</returns>
    private static async Task DisposeSilentlyCoreAsync(List<IAsyncDisposable> resources)
    {
        for (var index = resources.Count - 1; index >= 0; index--)
        {
            try
            {
                await resources[index].DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception disposalException)
            {
                _ = disposalException;
            }
        }

        resources.Clear();
    }

    /// <summary>Runs one asynchronous disposal stage and preserves the first failure.</summary>
    /// <param name="firstException">The first exception already captured.</param>
    /// <param name="stage">The disposal stage.</param>
    /// <returns>The first captured disposal exception.</returns>
    private static async ValueTask<Exception?> CaptureDisposalExceptionAsync(Exception? firstException, Func<ValueTask> stage)
    {
        try
        {
            await stage().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            firstException ??= exception;
        }

        return firstException;
    }

    /// <summary>Rethrows a captured cleanup failure while preserving its original stack.</summary>
    /// <param name="exception">The cleanup failure, or null when cleanup succeeded.</param>
    private static void RethrowDisposalFailure(Exception? exception)
    {
        switch (exception)
        {
            case null:
            {
                break;
            }

            default:
            {
                ExceptionDispatchInfo.Throw(exception);
                break;
            }
        }
    }

    /// <summary>Publishes the shared disposal task and captures resources for the single drain runner.</summary>
    /// <returns>The shared task, captured resources, and completion source for the runner when this call starts disposal.</returns>
    private DisposalStart BeginDispose()
    {
        lock (_syncRoot)
        {
            if (_disposeCompletion is not null)
            {
                return new(_disposeCompletion.Task, [], null);
            }

            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _disposeCompletion = completion;
            return new(completion.Task, TakeResources(), completion);
        }
    }

    /// <summary>Moves the current resource list out of the scope when disposal starts.</summary>
    /// <returns>The resources to dispose.</returns>
    private List<IAsyncDisposable> TakeResources()
    {
        _acceptingResources = false;
        var resources = new List<IAsyncDisposable>(_resources);
        _resources.Clear();
        return resources;
    }

    /// <summary>Captures the shared task and the optional runner state for a disposal start attempt.</summary>
    /// <param name="SharedTask">The shared task observed by every caller.</param>
    /// <param name="Resources">The resources for the single runner.</param>
    /// <param name="Completion">The completion source for the runner, or null for repeated callers.</param>
    private sealed record DisposalStart(Task SharedTask, List<IAsyncDisposable> Resources, TaskCompletionSource? Completion);
}
