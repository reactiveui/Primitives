// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// SingleReplaceableDisposable.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public class SingleReplaceableDisposable : IsDisposed
{
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    private readonly Action? _action;
    private IDisposable? _disposable;

    /// <summary>
    /// Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.
    /// </summary>
    /// <param name="action">The action.</param>
    public SingleReplaceableDisposable(Action? action = null) =>
        _action = action;

    /// <summary>
    /// Initializes a new instance of the <see cref="SingleReplaceableDisposable"/> class.
    /// </summary>
    /// <param name="disposable">The disposable.</param>
    /// <param name="action">The action to call before disposal.</param>
    public SingleReplaceableDisposable(IDisposable disposable, Action? action = null)
    {
        Create(disposable);
        _action = action;
    }

    /// <summary>
    /// Gets a value indicating whether this instance is disposed.
    /// </summary>
    /// <value>
    ///   <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed
    {
        get
        {
            return ReferenceEquals(Volatile.Read(ref _disposable), DisposedSentinel);
        }
    }

    /// <summary>
    /// Creates the specified disposable.
    /// </summary>
    /// <param name="disposable">The disposable.</param>
    public void Create(IDisposable disposable)
    {
        while (true)
        {
            var current = Volatile.Read(ref _disposable);
            if (ReferenceEquals(current, DisposedSentinel))
            {
                disposable?.Dispose();
                _action?.Invoke();
                return;
            }

            if (!ReferenceEquals(Interlocked.CompareExchange(ref _disposable, disposable, current), current))
            {
                continue;
            }

            current?.Dispose();
            return;
        }
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        var old = Interlocked.Exchange(ref _disposable, DisposedSentinel);
        if (ReferenceEquals(old, DisposedSentinel))
        {
            return;
        }

        old?.Dispose();
        _action?.Invoke();
    }

    private sealed class DisposedMarker : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
