// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Disposables;

/// <summary>
/// Single-assignment disposable slot.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public class SingleDisposable : IsDisposed
{
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    private readonly Action? _action;
    private IDisposable? _disposable;

    /// <summary>
    /// Initializes a new instance of the <see cref="SingleDisposable"/> class.
    /// </summary>
    /// <param name="action">Action to invoke before the assigned disposable is disposed.</param>
    public SingleDisposable(Action? action = null) => _action = action;

    /// <summary>
    /// Initializes a new instance of the <see cref="SingleDisposable"/> class.
    /// </summary>
    /// <param name="disposable">The disposable.</param>
    /// <param name="action">Action to invoke before the assigned disposable is disposed.</param>
    public SingleDisposable(IDisposable disposable, Action? action = null)
        : this(action) => Create(disposable);

    /// <summary>
    /// Gets a value indicating whether this instance is disposed.
    /// </summary>
    public bool IsDisposed
    {
        get
        {
            return ReferenceEquals(Volatile.Read(ref _disposable), DisposedSentinel);
        }
    }

    /// <summary>
    /// Assigns the disposable held by this slot.
    /// </summary>
    /// <param name="disposable">The disposable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposable"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The slot already has an assignment.</exception>
    public void Create(IDisposable disposable)
    {
        if (disposable == null)
        {
            throw new ArgumentNullException(nameof(disposable));
        }

        var current = Interlocked.CompareExchange(ref _disposable, disposable, null);
        if (current == null)
        {
            return;
        }

        if (ReferenceEquals(current, DisposedSentinel))
        {
            _action?.Invoke();
            disposable.Dispose();
            return;
        }

        throw new InvalidOperationException("The disposable slot has already been assigned.");
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        var disposable = Interlocked.Exchange(ref _disposable, DisposedSentinel);
        if (disposable == null || ReferenceEquals(disposable, DisposedSentinel))
        {
            return;
        }

        _action?.Invoke();
        disposable.Dispose();
    }

    private sealed class DisposedMarker : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
