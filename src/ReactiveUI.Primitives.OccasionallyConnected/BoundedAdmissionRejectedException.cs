// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a bounded queue admission rejection.</summary>
internal sealed class BoundedAdmissionRejectedException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="BoundedAdmissionRejectedException"/> class.</summary>
    internal BoundedAdmissionRejectedException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BoundedAdmissionRejectedException"/> class.</summary>
    /// <param name="message">The rejection message.</param>
    internal BoundedAdmissionRejectedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BoundedAdmissionRejectedException"/> class.</summary>
    /// <param name="message">The rejection message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal BoundedAdmissionRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
