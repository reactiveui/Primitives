// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Signals that a custom buffer overflow policy chose to wait for outbox capacity.</summary>
/// <remarks>The stream publish path catches this exception and waits for capacity; it never reaches callers.</remarks>
internal sealed class BufferOverflowBlockedException : InvalidOperationException
{
    /// <summary>The default blocked-admission message.</summary>
    private const string DefaultMessage = "The buffer overflow policy chose to wait for outbox capacity.";

    /// <summary>Initializes a new instance of the <see cref="BufferOverflowBlockedException"/> class.</summary>
    public BufferOverflowBlockedException()
        : base(DefaultMessage)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BufferOverflowBlockedException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    public BufferOverflowBlockedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="BufferOverflowBlockedException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The capacity failure that triggered the policy.</param>
    public BufferOverflowBlockedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
