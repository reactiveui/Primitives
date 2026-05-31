// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Base observer for boolean terminal operators that emit a single result or error.
/// </summary>
/// <typeparam name="T">The source value type.</typeparam>
public abstract class BooleanTerminalObserver<T> : SingleSourceObserver<T>
{
    /// <summary>
    /// The downstream observer.
    /// </summary>
    private readonly IObserver<bool> _observer;

    /// <summary>
    /// A value indicating whether the observer has terminated.
    /// </summary>
    private bool _done;

    /// <summary>
    /// Initializes a new instance of the <see cref="BooleanTerminalObserver{T}"/> class.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    protected BooleanTerminalObserver(IObserver<bool> observer) => _observer = observer;

    /// <summary>
    /// Gets a value indicating whether the observer has terminated.
    /// </summary>
    protected bool IsDone => _done;

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        if (_done)
        {
            return;
        }

        _done = true;
        try
        {
            _observer.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <summary>
    /// Emits the terminal boolean value and completes the observer.
    /// </summary>
    /// <param name="value">The terminal result.</param>
    protected void EmitCompleted(bool value)
    {
        if (_done)
        {
            return;
        }

        _done = true;
        try
        {
            _observer.OnNext(value);
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }
}
