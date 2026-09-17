// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Runs the notification gate, cancellation linking and disposal shared by every <see cref="IWitnessAsync{T}"/>.</summary>
/// <remarks>
/// A notification is dropped silently when the witness is disposed, its token is cancelled, or another thread holds the gate.
/// A failure from a hook never propagates back to the producer.
/// </remarks>
public static class WitnessAsync
{
    /// <summary>Delivers a value to <see cref="IWitnessAsync{T}.OnNextAsyncCore"/> under the notification gate.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="value">The value to be processed.</param>
    /// <param name="cancellationToken">A token that cancels the delivery; it is linked with the witness's disposal token.</param>
    /// <returns>A task that completes once the hook and its bookkeeping have run.</returns>
    public static ValueTask OnNextAsync<T>(IWitnessAsync<T> witness, T value, CancellationToken cancellationToken)
    {
        if (!witness.TryEnterOnSomethingCall(cancellationToken, out var scope))
        {
            return default;
        }

        ValueTask core;
        try
        {
            core = witness.OnNextAsyncCore(value, scope.Token);
        }
        catch (OperationCanceledException)
        {
            scope.Dispose();
            _ = witness.Witness.ExitOnSomethingCall();
            return default;
        }
        catch (Exception e)
        {
            return OnNextAsyncSlowAfterSyncThrow(witness, e, scope);
        }

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            _ = witness.Witness.ExitOnSomethingCall();
            return default;
        }

        return OnNextAsyncSlow(witness, core, scope);
    }

    /// <summary>Routes a non-terminal error to <see cref="IWitnessAsync{T}.OnErrorResumeAsyncCore"/> under the notification gate.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="error">The exception that triggered the error handling logic.</param>
    /// <param name="cancellationToken">A token that cancels the handling; it is linked with the witness's disposal token.</param>
    /// <returns>A task that completes once the error has been handled.</returns>
    public static ValueTask OnErrorResumeAsync<T>(IWitnessAsync<T> witness, Exception error, CancellationToken cancellationToken)
    {
        if (!witness.TryEnterOnSomethingCall(cancellationToken, out var scope))
        {
            return default;
        }

        // Callback failures are captured in the returned ValueTask.
        var core = RouteObserverErrorCoreAsync(witness, error, scope.Token);

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            _ = witness.Witness.ExitOnSomethingCall();
            return default;
        }

        return OnErrorResumeAsyncSlow(witness, core, scope);
    }

    /// <summary>Delivers the terminal result to <see cref="IWitnessAsync{T}.OnCompletedAsyncCore"/> and then disposes the witness.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="result">The result of the completed operation.</param>
    /// <returns>A task that completes once the hook has run and the witness is torn down.</returns>
    /// <remarks>A failure from the hook goes to <see cref="UnhandledExceptionHandler"/>; disposal runs either way.</remarks>
    [DebuggerStepThrough]
    public static ValueTask OnCompletedAsync<T>(IWitnessAsync<T> witness, Result result)
    {
        if (!witness.TryEnterOnSomethingCall(CancellationToken.None, out var scope))
        {
            return default;
        }

        ValueTask core;
        try
        {
            core = witness.OnCompletedAsyncCore(result);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
            scope.Dispose();
            return witness.CompleteOrChainDispose();
        }

        if (core.IsCompletedSuccessfully)
        {
            scope.Dispose();
            return witness.CompleteOrChainDispose();
        }

        return OnCompletedAsyncSlow(witness, core, scope);
    }

    /// <summary>Disposes the witness from within its own in-flight notification, skipping the in-flight-call wait.</summary>
    /// <param name="witness">The witness.</param>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    /// <remarks>Waiting would self-deadlock once the notification continuation has hopped threads.</remarks>
    [DebuggerStepThrough]
    public static ValueTask DisposeFromNotificationAsync(IWitnessState witness)
    {
        witness.Witness.MarkDisposingFromNotification();
        return witness.DisposeAsync();
    }

    /// <summary>Claims disposal, waits for active notifications, and releases the cancellation link, the dispose source and the source subscription.</summary>
    /// <param name="witness">The witness.</param>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    /// <remarks>
    /// Idempotent. Disposal waits out a notification in flight on another thread, and a failure while releasing the source
    /// subscription is reported to <see cref="UnhandledExceptionHandler"/> rather than thrown.
    /// </remarks>
    [DebuggerStepThrough]
    public static async ValueTask DisposeStateAsync(IWitnessState witness)
    {
        witness.Witness.EnsureLinked(witness);
        if (!witness.Witness.TryClaimDisposal())
        {
            return;
        }

        var allOnSomethingCallsCompleted = witness.Witness.PublishWaiterForForeignCall();

        // Tokens requested after disposal are already cancelled.
        var cts = witness.Witness.DisposeSource;
        if (cts is not null)
        {
            _ = await ConcurrencyRaceHelpers.TryCancelAsync(cts).ConfigureAwait(false);
        }

        if (allOnSomethingCallsCompleted is not null)
        {
            await allOnSomethingCallsCompleted.ConfigureAwait(false);
        }

        await witness.Witness.DisposeExternalLinkAsync().ConfigureAwait(false);
        witness.Witness.DisposeCancellationSource();

        try
        {
            await witness.Witness.DisposeSourceSubscriptionAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Creates a disposal source for a witness.</summary>
    /// <returns>A new cancellation token source.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CancellationTokenSource CreateDisposeSource() => new();

    /// <summary>Runs the error hook, reporting anything it throws, and the original error when the call is cancelled.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="error">The exception that triggered error handling.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    internal static async ValueTask RouteObserverErrorCoreAsync<T>(IWitnessAsync<T> witness, Exception error, CancellationToken cancellationToken)
    {
        try
        {
            if (cancellationToken.IsCancellationRequested)
            {
                UnhandledExceptionHandler.ReportUnhandledException(error);
                return;
            }

            await witness.OnErrorResumeAsyncCore(error, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            UnhandledExceptionHandler.ReportUnhandledException(error);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
    }

    /// <summary>Async continuation for <see cref="OnNextAsync{T}"/> when the hook's task is incomplete.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="core">The pending hook task.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A task that completes once the hook completes and bookkeeping has run.</returns>
    private static async ValueTask OnNextAsyncSlow<T>(IWitnessAsync<T> witness, ValueTask core, LinkedTokenScope scope)
    {
        try
        {
            await core.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation does not report a handler failure.
        }
        catch (Exception e)
        {
            await RouteObserverErrorCoreAsync(witness, e, scope.Token).ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
            _ = witness.Witness.ExitOnSomethingCall();
        }
    }

    /// <summary>Routes a synchronous throw from the value hook through error handling, out of line so <see cref="OnNextAsync{T}"/> needs no state machine.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="error">The exception thrown by the hook.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A task that completes once error handling and bookkeeping have run.</returns>
    private static async ValueTask OnNextAsyncSlowAfterSyncThrow<T>(IWitnessAsync<T> witness, Exception error, LinkedTokenScope scope)
    {
        try
        {
            await RouteObserverErrorCoreAsync(witness, error, scope.Token).ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
            _ = witness.Witness.ExitOnSomethingCall();
        }
    }

    /// <summary>Async continuation for <see cref="OnErrorResumeAsync{T}"/> when the hook's task is incomplete.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="core">The pending hook task.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A task that completes once the hook completes and bookkeeping has run.</returns>
    private static async ValueTask OnErrorResumeAsyncSlow<T>(IWitnessAsync<T> witness, ValueTask core, LinkedTokenScope scope)
    {
        try
        {
            await core.ConfigureAwait(false);
        }
        finally
        {
            scope.Dispose();
            _ = witness.Witness.ExitOnSomethingCall();
        }
    }

    /// <summary>Async continuation for <see cref="OnCompletedAsync{T}"/> when the hook's task is incomplete.</summary>
    /// <typeparam name="T">The type of the elements received by the witness.</typeparam>
    /// <param name="witness">The witness.</param>
    /// <param name="core">The pending hook task.</param>
    /// <param name="scope">The linked-token scope to release on completion.</param>
    /// <returns>A task that completes once the hook, bookkeeping, and any required dispose have run.</returns>
    private static async ValueTask OnCompletedAsyncSlow<T>(IWitnessAsync<T> witness, ValueTask core, LinkedTokenScope scope)
    {
        try
        {
            await core.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            UnhandledExceptionHandler.ReportUnhandledException(e);
        }
        finally
        {
            scope.Dispose();
        }

        if (witness.Witness.ExitOnSomethingCall())
        {
            await witness.DisposeAsync().ConfigureAwait(false);
        }
    }
}
