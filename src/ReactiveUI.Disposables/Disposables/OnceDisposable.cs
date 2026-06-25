// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// A disposable holder whose inner disposable can be set exactly once.
/// Replaces <c>SingleAssignmentDisposable</c>. Subsequent assignments throw
/// <see cref="InvalidOperationException"/>; if the holder has been disposed before
/// assignment, the supplied disposable is disposed immediately and no exception is thrown.
/// </summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class OnceDisposable : IsDisposed
{
    /// <summary>Sentinel value indicating the object has been disposed.</summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>The current inner disposable.</summary>
    private IDisposable? _current;

    /// <summary>Gets a value indicating whether a disposable has been assigned.</summary>
    public bool IsAssigned => Volatile.Read(ref _current) is not null;

    /// <summary>Gets a value indicating whether this instance has been disposed.</summary>
    public bool IsDisposed => ReferenceEquals(Volatile.Read(ref _current), DisposedSentinel);

    /// <summary>Gets or sets the inner disposable. Setting more than once throws.</summary>
    public IDisposable? Disposable
    {
        get
        {
            var current = Volatile.Read(ref _current);
            return ReferenceEquals(current, DisposedSentinel) ? null : current;
        }

        set
        {
            var previous = Interlocked.CompareExchange(ref _current, value, null);
            if (previous is null)
            {
                return;
            }

            if (ReferenceEquals(previous, DisposedSentinel))
            {
                value?.Dispose();
                return;
            }

            throw new InvalidOperationException("Disposable already assigned.");
        }
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Dispose()
    {
        var previous = Interlocked.Exchange(ref _current, DisposedSentinel);
        if (previous is null || ReferenceEquals(previous, DisposedSentinel))
        {
            return;
        }

        previous.Dispose();
    }

    /// <summary>Disposable marker for disposed instances.</summary>
    private sealed class DisposedMarker : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose()
        {
            // Intentionally empty.
        }
    }
}
