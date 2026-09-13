// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides static helpers for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
internal sealed partial class OccasionallyConnectedStream<TState, TInput>
{
    /// <summary>The nominal object overhead charged for retained notification envelopes.</summary>
    private const long NotificationObjectOverheadBytes = 32;

    /// <summary>The fault envelope and owned diagnostic exception retained for one fault.</summary>
    private const int FaultNotificationObjectCount = 2;

    /// <summary>The byte count retained by a GUID field.</summary>
    private const long GuidSizeBytes = 16;

    /// <summary>The maximum retained diagnostic exception type name length.</summary>
    private const int MaximumDiagnosticTypeNameLength = 256;

    /// <summary>Gets the explicit subscription identity configured on the definition.</summary>
    /// <param name="definition">The stream definition.</param>
    /// <returns>The preferred identity, or null when identity must be recovered or allocated.</returns>
    private static SubscriptionId? GetPreferredSubscriptionId(StreamDefinition<TState, TInput> definition) =>
        definition.SubscriptionId ?? definition.Subscription?.SubscriptionId;

    /// <summary>Creates a persisted operation policy from publish options.</summary>
    /// <param name="options">The publish options.</param>
    /// <returns>The operation policy.</returns>
    private static OperationPolicy CreatePolicy(RemotePublishOptions? options)
    {
        if (options is null)
        {
            return OperationPolicy.Default;
        }

        return new(
            options.DeliveryGuarantee,
            options.Durable ? OperationDurability.Durable : OperationDurability.Volatile,
            options.Priority,
            options.ConflictPolicy);
    }

    /// <summary>Gets the retained notification size for a payload-backed notification.</summary>
    /// <param name="payload">The payload backing the notification.</param>
    /// <returns>The byte size charged to observer queues.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetNotificationSize(PayloadEnvelope payload) =>
        Math.Max(
            MinimumNotificationSizeBytes,
            payload.PayloadLength
            + NotificationObjectOverheadBytes
            + sizeof(int)
            + GetTextSize(payload.ContractId)
            + GetTextSize(payload.ContentType)
            + GetTextSize(payload.PayloadHash));

    /// <summary>Gets the retained notification size for a remote message snapshot.</summary>
    /// <param name="remoteEvent">The remote event backing the notification.</param>
    /// <returns>The byte size charged to observer queues.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetRemoteNotificationSize(RemoteEvent remoteEvent) =>
        GetNotificationSize(remoteEvent.Payload)
        + NotificationObjectOverheadBytes
        + GetGuidSize()
        + GetTextSize(remoteEvent.StreamId.Value)
        + GetTextSize(remoteEvent.ServerCursor)
        + sizeof(long);

    /// <summary>Gets the retained notification size for an operation status snapshot.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="receipt">The local publish receipt.</param>
    /// <returns>The byte size charged to observer queues.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetOperationStatusNotificationSize(StreamId streamId, PublishReceipt receipt) =>
        Math.Max(
            MinimumNotificationSizeBytes,
            NotificationObjectOverheadBytes
            + GetGuidSize()
            + GetTextSize(streamId.Value)
            + sizeof(int)
            + sizeof(long)
            + GetTextSize(receipt.State.ToString()));

    /// <summary>Gets the retained notification size for a bounded fault diagnostic.</summary>
    /// <param name="fault">The fault notification.</param>
    /// <param name="diagnostic">The owned diagnostic exception retained by the fault.</param>
    /// <returns>The byte size charged to observer queues.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetFaultNotificationSize(OccasionallyConnectedFault fault, InvalidOperationException diagnostic) =>
        Math.Max(
            MinimumNotificationSizeBytes,
            (NotificationObjectOverheadBytes * FaultNotificationObjectCount)
            + GetTextSize(fault.Code)
            + GetTextSize(fault.Message)
            + GetTextSize(diagnostic.Message));

    /// <summary>Creates a finite type-only diagnostic without retaining caller messages, data, or exception graphs.</summary>
    /// <param name="exception">The observed exception.</param>
    /// <returns>The bounded diagnostic exception.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static InvalidOperationException CreateDiagnosticException(Exception exception) =>
        new(TrimDiagnostic(exception.GetType().ToString(), MaximumDiagnosticTypeNameLength));

    /// <summary>Gets a nominal retained size for diagnostic text.</summary>
    /// <param name="text">The text.</param>
    /// <returns>The byte size.</returns>
    private static long GetTextSize(string text) =>
        NotificationObjectOverheadBytes + ((long)text.Length * sizeof(char));

    /// <summary>Gets the byte size retained by a GUID field.</summary>
    /// <returns>The GUID byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetGuidSize() =>
        GuidSizeBytes;

    /// <summary>Determines whether a task completed successfully on every supported target framework.</summary>
    /// <param name="task">The inspected task.</param>
    /// <returns><see langword="true"/> when the task ran to completion.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsCompletedSuccessfully(Task task) =>
        task.Status == TaskStatus.RanToCompletion;

    /// <summary>Runs notification scheduling after the facade lock has been released.</summary>
    /// <param name="schedules">The scheduling callbacks.</param>
    private static void RunNotificationSchedules(List<Action> schedules)
    {
        for (var i = 0; i < schedules.Count; i++)
        {
            schedules[i]();
        }
    }

    /// <summary>Trims diagnostic text to a bounded length.</summary>
    /// <param name="text">The source text.</param>
    /// <param name="maximumLength">The maximum retained length.</param>
    /// <returns>The bounded text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string TrimDiagnostic(string text, int maximumLength) =>
        text.Substring(0, Math.Min(text.Length, maximumLength));

    /// <summary>Waits for shared initialization while observing caller cancellation.</summary>
    /// <param name="task">The shared initialization task.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The recovered state.</returns>
    private static Task<LocalStreamCommitter<TState, TInput>> WaitForInitializationAsync(
        Task<LocalStreamCommitter<TState, TInput>> task,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return cancellationToken.CanBeCanceled && !task.IsCompleted
            ? task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken)
            : task;
    }

    /// <summary>Waits for shared lifecycle convergence while observing caller cancellation.</summary>
    /// <param name="task">The shared lifecycle task.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The wait task.</returns>
    private static Task WaitForLifecycleAsync(Task task, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return cancellationToken.CanBeCanceled && !task.IsCompleted
            ? task.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken)
            : task;
    }

    /// <summary>Runs an asynchronous cleanup step and preserves the first failure.</summary>
    /// <param name="cleanup">The callback that starts the cleanup task.</param>
    /// <param name="failure">The existing failure.</param>
    /// <returns>The first observed failure.</returns>
    private static async ValueTask<Exception?> CaptureFailureAsync(Func<Task> cleanup, Exception? failure)
    {
        try
        {
            await cleanup().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            return failure ?? exception;
        }

        return failure;
    }
}
