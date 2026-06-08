// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>Represents the WitnessBase class.</summary>
/// <typeparam name="TSource">The TSource type.</typeparam>
/// <typeparam name="TResult">The TResult type.</typeparam>
internal abstract class WitnessBase<TSource, TResult> : IDisposable, IObserver<TSource>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private IDisposable? _cancel;

    /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="WitnessBase{TSource,TResult}"/> class.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    private protected WitnessBase(IObserver<TResult> observer, IDisposable cancel)
    {
        _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
        Observer = observer;
    }

    /// <summary>
    /// Gets the downstream observer. Set once in the constructor and read without a memory barrier
    /// on the per-value path; teardown is handled by disposing the upstream subscription rather
    /// than by swapping this reference.
    /// </summary>
    protected internal IObserver<TResult> Observer { get; }

    /// <summary>Executes the OnNext operation.</summary>
    /// <param name="value">The value.</param>
    public abstract void OnNext(TSource value);

    /// <summary>Executes the OnError operation.</summary>
    /// <param name="error">The error value.</param>
    public abstract void OnError(Exception error);

    /// <summary>Executes the OnCompleted operation.</summary>
    public abstract void OnCompleted();

    /// <summary>Executes the Dispose operation.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Executes the Dispose operation.</summary>
    /// <param name="disposing">The disposing value.</param>
    protected virtual void Dispose(bool disposing)
    {
        // Atomic run-once latch so concurrent disposal cannot double-tear-down.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (!disposing)
        {
            return;
        }

        var target = Interlocked.Exchange(ref _cancel, null);
        target?.Dispose();
    }
}
