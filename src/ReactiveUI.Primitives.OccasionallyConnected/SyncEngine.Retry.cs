// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Retry classification helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Classifies an engine failure for retry policy evaluation.</summary>
    /// <param name="exception">The observed failure.</param>
    /// <returns>The retry failure.</returns>
    private static RetryFailure ClassifyRetryFailure(Exception exception) =>
        exception switch
        {
            IRemoteTransportFailure remote => remote.RetryFailure,
            TimeoutException or System.IO.IOException or ObjectDisposedException => RetryFailure.Transient(),
            PayloadSchemaException schema when schema.Reason == PayloadSchemaFailureReason.PayloadTooLarge => new(RetryFailureKind.PayloadTooLarge),
            PayloadSchemaException => new(RetryFailureKind.SchemaIncompatible),
            SyncBatchValidationException or ArgumentException or InvalidOperationException or NotSupportedException => new(RetryFailureKind.ValidationRejected),
            UnauthorizedAccessException => new(RetryFailureKind.AuthorizationDenied),
            _ => new(RetryFailureKind.ValidationRejected),
        };

    /// <summary>Determines whether a classified retry failure may be retried by policy.</summary>
    /// <param name="failure">The retry failure.</param>
    /// <returns><see langword="true"/> when policy may retry the failure.</returns>
    private static bool IsRetryableFailure(RetryFailure failure) =>
        failure.Kind is RetryFailureKind.Transient or RetryFailureKind.AmbiguousTransportOutcome or RetryFailureKind.Authentication;

    /// <summary>Provides jitter for engine retry paths.</summary>
    internal sealed class EngineRetryRandomSource : IRetryRandomSource
    {
        /// <summary>The byte count needed for a 32-bit random sample.</summary>
        private const int SampleByteCount = 4;

        /// <summary>The random number generator used for retry jitter.</summary>
        private static readonly System.Security.Cryptography.RandomNumberGenerator Generator =
            System.Security.Cryptography.RandomNumberGenerator.Create();

        /// <summary>Gets the singleton retry random source.</summary>
        internal static EngineRetryRandomSource Instance { get; } = new();

        /// <inheritdoc/>
        public double NextDouble()
        {
            var bytes = new byte[SampleByteCount];
            Generator.GetBytes(bytes);
            var sample = BitConverter.ToUInt32(bytes, 0);
            return (double)sample / uint.MaxValue;
        }
    }
}
