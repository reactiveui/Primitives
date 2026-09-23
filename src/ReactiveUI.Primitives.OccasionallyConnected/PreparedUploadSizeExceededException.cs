// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies a prepared upload body that exceeds the negotiated encoded byte limit.</summary>
internal sealed class PreparedUploadSizeExceededException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="PreparedUploadSizeExceededException"/> class.</summary>
    internal PreparedUploadSizeExceededException()
        : this("The prepared upload exceeds the configured encoded byte limit.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PreparedUploadSizeExceededException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    internal PreparedUploadSizeExceededException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PreparedUploadSizeExceededException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal PreparedUploadSizeExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PreparedUploadSizeExceededException"/> class.</summary>
    /// <param name="encodedSizeBytes">The encoded size reported by the prepared transport handle.</param>
    /// <param name="maximumEncodedSizeBytes">The negotiated maximum encoded size.</param>
    internal PreparedUploadSizeExceededException(long encodedSizeBytes, long maximumEncodedSizeBytes)
        : this()
    {
        EncodedSizeBytes = encodedSizeBytes;
        MaximumEncodedSizeBytes = maximumEncodedSizeBytes;
    }

    /// <summary>Gets the encoded size reported by the prepared transport handle.</summary>
    internal long EncodedSizeBytes { get; }

    /// <summary>Gets the negotiated maximum encoded size.</summary>
    internal long MaximumEncodedSizeBytes { get; }
}
