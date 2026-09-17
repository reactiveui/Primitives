// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides static helpers for <see cref="OccasionallyConnectedInputProducer{TInput}"/>.</summary>
internal sealed partial class OccasionallyConnectedInputProducer<TInput>
{
    /// <summary>The stable fault code for capture provider failures.</summary>
    private const string InputCaptureFaultCode = "OC.Stream.InputCapture";

    /// <summary>The stable fault code for producer overflow.</summary>
    private const string InputOverflowFaultCode = "OC.Stream.InputOverflow";

    /// <summary>The stable fault code for publish callback failures.</summary>
    private const string InputPublishFaultCode = "OC.Stream.InputPublish";

    /// <summary>The stable fault code for producer terminal errors.</summary>
    private const string InputProducerFaultCode = "OC.Stream.InputProducer";

    /// <summary>The minimum retained byte charge for an admitted input item.</summary>
    private const long MinimumRetainedInputBytes = 1;

    /// <summary>The nominal object overhead charged for retained input envelopes.</summary>
    private const long EnvelopeObjectOverheadBytes = 32;

    /// <summary>The maximum retained diagnostic exception type name length.</summary>
    private const int MaximumDiagnosticTypeNameLength = 256;

    /// <summary>Gets the retained byte count for an owned payload envelope.</summary>
    /// <param name="payload">The captured payload envelope.</param>
    /// <returns>The retained byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetRetainedEnvelopeByteCount(PayloadEnvelope payload) =>
        Math.Max(
            MinimumRetainedInputBytes,
            payload.PayloadLength
            + EnvelopeObjectOverheadBytes
            + sizeof(int)
            + GetTextSize(payload.ContractId)
            + GetTextSize(payload.ContentType)
            + GetTextSize(payload.PayloadHash));

    /// <summary>Creates a finite type-only diagnostic without retaining caller exception graphs.</summary>
    /// <param name="exception">The observed exception.</param>
    /// <returns>The bounded diagnostic exception.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static InvalidOperationException CreateDiagnosticException(Exception exception) =>
        new(TrimDiagnostic(exception.GetType().ToString(), MaximumDiagnosticTypeNameLength));

    /// <summary>Determines whether an exception should propagate instead of becoming diagnostic noise.</summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns><see langword="true"/> when the exception is fatal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsFatalException(Exception exception) =>
        exception is
            StackOverflowException or
            AccessViolationException or
            AppDomainUnloadedException or
            BadImageFormatException or
            CannotUnloadAppDomainException or
            InvalidProgramException or
            ThreadAbortException or
            OutOfMemoryException and not InsufficientMemoryException;

    /// <summary>Gets a nominal retained size for diagnostic text.</summary>
    /// <param name="text">The text.</param>
    /// <returns>The byte size.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetTextSize(string text) =>
        EnvelopeObjectOverheadBytes + ((long)text.Length * sizeof(char));

    /// <summary>Trims diagnostic text to a bounded length.</summary>
    /// <param name="text">The source text.</param>
    /// <param name="maximumLength">The maximum retained length.</param>
    /// <returns>The bounded text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string TrimDiagnostic(string text, int maximumLength) =>
        text.Substring(0, Math.Min(text.Length, maximumLength));
}
