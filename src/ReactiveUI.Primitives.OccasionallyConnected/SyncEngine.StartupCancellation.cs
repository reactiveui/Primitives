// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Owns cancellation of shared transport startup independently from caller waits.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>The borrowed cancellation view of the locally owned startup generation, protected by the engine gate.</summary>
    private IStartupCancellation? _startupCancellation;

    /// <summary>Exposes cancellation without transferring ownership of the startup lifetime.</summary>
    private interface IStartupCancellation
    {
        /// <summary>Gets whether stop intent was captured under the engine gate.</summary>
        bool StopRequested { get; }

        /// <summary>Gets the shared cancellation completion, including callback failures.</summary>
        Task CancellationTask { get; }

        /// <summary>Records cancellation intent while the engine gate is held.</summary>
        void RecordStop();

        /// <summary>Runs cancellation callbacks and completes their shared wait.</summary>
        /// <returns>The cancellation task.</returns>
        Task CancelAsync();
    }

    /// <summary>Starts a transport generation with independently owned cancellation.</summary>
    /// <returns>The shared startup task.</returns>
    /// <exception cref="OperationCanceledException">An accepted stop superseded startup.</exception>
    private async ValueTask StartCoreAsync()
    {
        StartupCancellationOwner cancellation;
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            if (_admissionState == EngineAdmissionState.Stopping)
            {
                throw new OperationCanceledException("Startup was superseded by an accepted stop.");
            }

            cancellation = new();
            _startupCancellation = cancellation;
            _admissionState = EngineAdmissionState.Starting;
        }

        try
        {
            await StartWithCancellationAsync(cancellation).ConfigureAwait(false);
        }
        finally
        {
            Task cancellationTask;
            lock (_gate)
            {
                cancellationTask = cancellation.StopRequested ? cancellation.CancellationTask : Task.CompletedTask;
                if (!cancellation.StopRequested)
                {
                    _startupCancellation = null;
                }
            }

            try
            {
                await cancellationTask.ConfigureAwait(false);
            }
            finally
            {
                lock (_gate)
                {
                    _startupCancellation = null;
                }

                cancellation.Dispose();
            }
        }
    }

    /// <summary>Cancels current startup without running callbacks under the engine gate.</summary>
    /// <returns>The cancellation task, including callback failures.</returns>
    private Task CancelStartupAsync()
    {
        IStartupCancellation? cancellation;
        IStartupCancellation? launchCancellation = null;
        Task completion;
        lock (_gate)
        {
            cancellation = _startupCancellation;
            if (cancellation is null)
            {
                completion = Task.CompletedTask;
            }
            else
            {
                completion = cancellation.CancellationTask;
                if (!cancellation.StopRequested)
                {
                    cancellation.RecordStop();
                    launchCancellation = cancellation;
                }
            }
        }

        if (launchCancellation is not null)
        {
            _ = launchCancellation.CancelAsync();
        }

        return completion;
    }

    /// <summary>Retains a startup token until both startup and cancellation callbacks finish.</summary>
    private sealed class StartupCancellationOwner : IStartupCancellation, IDisposable
    {
        /// <summary>The token source owned by this startup generation.</summary>
        private readonly CancellationTokenSource _source = new();

        /// <summary>The shared cancellation completion, including callback failures.</summary>
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets whether stop intent was captured under the engine gate.</summary>
        public bool StopRequested { get; private set; }

        /// <inheritdoc/>
        public Task CancellationTask => _completion.Task;

        /// <summary>Gets the token passed to transport startup.</summary>
        internal CancellationToken Token => _source.Token;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _source.Dispose();

        /// <summary>Records cancellation intent while the engine gate is held.</summary>
        public void RecordStop() => StopRequested = true;

        /// <summary>Runs cancellation callbacks outside engine gates and completes their shared wait.</summary>
        /// <returns>The cancellation task.</returns>
        public async Task CancelAsync()
        {
            try
            {
                await _source.CancelAsync().ConfigureAwait(false);
                _ = _completion.TrySetResult(true);
            }
            catch (Exception exception)
            {
                _ = _completion.TrySetException(exception);
                _ = _completion.Task.Exception;
            }
        }
    }
}
