// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// Holds a replaceable disposable and disposes the previous assignment when changed.
/// </summary>
public sealed class SerialDisposable : IDisposable
{
    /// <summary>
    /// Marker used after disposal.
    /// </summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>
    /// Current disposable assignment.
    /// </summary>
    private IDisposable? _current;

    /// <summary>
    /// Gets or sets the current disposable.
    /// </summary>
    public IDisposable? Disposable
    {
        get
        {
            var current = Volatile.Read(ref _current);
            return ReferenceEquals(current, DisposedSentinel) ? null : current;
        }

        set
        {
            IDisposable? current;
            do
            {
                current = Volatile.Read(ref _current);
                if (ReferenceEquals(current, DisposedSentinel))
                {
                    value?.Dispose();
                    return;
                }
            }
            while (!ReferenceEquals(Interlocked.CompareExchange(ref _current, value, current), current));

            current?.Dispose();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        var current = Interlocked.Exchange(ref _current, DisposedSentinel);
        if (current == null || ReferenceEquals(current, DisposedSentinel))
        {
            return;
        }

        current.Dispose();
    }

    /// <summary>
    /// Disposable marker for disposed slots.
    /// </summary>
    private sealed class DisposedMarker : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
