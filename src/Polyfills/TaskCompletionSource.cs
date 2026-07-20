// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

// Polyfill implementation adapted from SimonCropp/Polyfill (https://github.com/SimonCropp/Polyfill).

#if !NET5_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;

namespace System.Threading.Tasks;

/// <summary>Polyfill for the non-generic <see cref="TaskCompletionSource"/> introduced in .NET 5, backed by a <see cref="TaskCompletionSource{TResult}"/>.</summary>
[SuppressMessage("Performance", "CA1812", Justification =
    "Broadcast polyfill; not instantiated in every consuming leaf.")]
internal sealed class TaskCompletionSource
{
    /// <summary>The underlying generic completion source that backs this non-generic facade.</summary>
    private readonly TaskCompletionSource<bool> _inner;

    /// <summary>Initializes a new instance of the <see cref="TaskCompletionSource"/> class.</summary>
    [SuppressMessage("Concurrency", "PSH1302", Justification =
        "BCL-parity polyfill; must match the framework ctor's TaskCreationOptions.None default, not force async continuations.")]
    public TaskCompletionSource() => _inner = new();

    /// <summary>Transitions the underlying task to the <see cref="TaskStatus.RanToCompletion"/> state.</summary>
    internal void SetResult() => _inner.SetResult(true);

    /// <summary>Attempts to transition the underlying task to the <see cref="TaskStatus.RanToCompletion"/> state.</summary>
    /// <returns><see langword="true"/> if the operation was successful; otherwise <see langword="false"/>.</returns>
    internal bool TrySetResult() => _inner.TrySetResult(true);

    /// <summary>Transitions the underlying task to the <see cref="TaskStatus.Faulted"/> state with the specified exception.</summary>
    /// <param name="exception">The exception to bind to the task.</param>
    internal void SetException(Exception exception) => _inner.SetException(exception);

    /// <summary>Attempts to transition the underlying task to the <see cref="TaskStatus.Faulted"/> state with the specified exception.</summary>
    /// <param name="exception">The exception to bind to the task.</param>
    /// <returns><see langword="true"/> if the operation was successful; otherwise <see langword="false"/>.</returns>
    internal bool TrySetException(Exception exception) => _inner.TrySetException(exception);

    /// <summary>Transitions the underlying task to the <see cref="TaskStatus.Canceled"/> state.</summary>
    internal void SetCanceled() => _inner.TrySetCanceled();

    /// <summary>Attempts to transition the underlying task to the <see cref="TaskStatus.Canceled"/> state.</summary>
    /// <returns><see langword="true"/> if the operation was successful; otherwise <see langword="false"/>.</returns>
    [SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "Distinct BCL-parity surface members with different contracts and return types: SetCanceled is void and "
            + "TrySetCanceled returns bool. They share the same backing call here but must track the framework surface "
            + "independently, so they are deliberately kept as separate members rather than one forwarding to the other.")]
    internal bool TrySetCanceled() => _inner.TrySetCanceled();

    /// <summary>Attempts to transition the underlying task to the <see cref="TaskStatus.Canceled"/> state for the specified token.</summary>
    /// <param name="cancellationToken">The token associated with the cancellation.</param>
    /// <returns><see langword="true"/> if the operation was successful; otherwise <see langword="false"/>.</returns>
    internal bool TrySetCanceled(CancellationToken cancellationToken) => _inner.TrySetCanceled(cancellationToken);
}
#endif
