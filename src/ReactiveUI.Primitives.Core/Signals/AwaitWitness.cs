// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Represents the AwaitWitness class.</summary>
/// <typeparam name="T">The Type.</typeparam>
public sealed class AwaitWitness<T> : IObserver<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly SynchronizationContext? _context;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Action _callback;

    /// <summary>Initializes a new instance of the <see cref="AwaitWitness{T}"/> class.</summary>
    /// <param name="callback">The callback value.</param>
    /// <param name="originalContext">The originalContext value.</param>
    public AwaitWitness(Action callback, bool originalContext)
    {
        if (originalContext)
        {
            _context = SynchronizationContext.Current;
        }

        _callback = callback;
    }

    /// <summary>Executes the OnCompleted operation.</summary>
    public void OnCompleted() => InvokeOnOriginalContext();

    /// <summary>Executes the OnError operation.</summary>
    /// <param name="error">The error value.</param>
    public void OnError(Exception error) => InvokeOnOriginalContext();

    /// <summary>Executes the OnNext operation.</summary>
    /// <param name="value">The value.</param>
    public void OnNext(T value)
    {
    }

    /// <summary>Executes the InvokeOnOriginalContext operation.</summary>
    private void InvokeOnOriginalContext()
    {
        if (_context is not null)
        {
            _context.Post(static state => ((Action?)state)?.Invoke(), _callback);
        }
        else
        {
            _callback();
        }
    }
}
