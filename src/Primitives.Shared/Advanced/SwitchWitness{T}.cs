// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Mediates latest-inner subscription switching for <see cref="SwitchSignal{T}"/>.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SwitchWitness<T> : IDisposable
{
    /// <summary>The synchronization gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The current inner source version.</summary>
    private int _version;

    /// <summary>Initializes a new instance of the <see cref="SwitchWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public SwitchWitness(IObserver<T> observer) => Observer = observer;

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<T> Observer { get; }

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the active inner subscription slot.</summary>
    private SingleReplaceableDisposable InnerSlot { get; } = new();

    /// <summary>Gets or sets a value indicating whether the outer source completed.</summary>
    private bool IsOuterCompleted { get; set; }

    /// <summary>Gets or sets a value indicating whether an inner source is active.</summary>
    private bool IsInnerActive { get; set; }

    /// <summary>Gets or sets a value indicating whether a terminal notification has been emitted.</summary>
    private bool IsDone { get; set; }

    /// <inheritdoc/>
    public void Dispose()
    {
        InnerSlot.Dispose();
        Subscriptions.Dispose();
    }

    /// <summary>Starts observing the outer source.</summary>
    /// <param name="sources">The outer source.</param>
    /// <returns>The observer that owns the subscriptions.</returns>
    public SwitchWitness<T> Run(IObservable<IObservable<T>> sources)
    {
        Subscriptions.Add(InnerSlot);
        Subscriptions.Add(sources.Subscribe(OnSource, OnOuterError, OnOuterCompleted));
        return this;
    }

    /// <summary>Switches to a new inner source.</summary>
    /// <param name="source">The new inner source.</param>
    private void OnSource(IObservable<T> source)
    {
        int current;
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            current = _version + 1;
            Volatile.Write(ref _version, current);
            IsInnerActive = true;
        }

        InnerSlot.Create(source.Subscribe(
            value => OnNext(current, value),
            error => OnError(current, error),
            () => OnCompleted(current)));
    }

    /// <summary>Marks the outer source complete.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsOuterCompleted = true;
            TryComplete();
        }
    }

    /// <summary>Forwards the outer source error once.</summary>
    /// <param name="error">The error to forward.</param>
    private void OnOuterError(Exception error)
    {
        lock (_gate)
        {
            if (IsDone)
            {
                return;
            }

            IsDone = true;
            Observer.OnError(error);
        }
    }

    /// <summary>Forwards a current inner value.</summary>
    /// <param name="version">The inner source version.</param>
    /// <param name="value">The value to forward.</param>
    private void OnNext(int version, T value)
    {
        lock (_gate)
        {
            if (IsDone || version != _version)
            {
                return;
            }

            Observer.OnNext(value);
        }
    }

    /// <summary>Forwards a current inner error.</summary>
    /// <param name="version">The inner source version.</param>
    /// <param name="error">The error to forward.</param>
    private void OnError(int version, Exception error)
    {
        lock (_gate)
        {
            if (IsDone || version != _version)
            {
                return;
            }

            IsDone = true;
            Observer.OnError(error);
        }
    }

    /// <summary>Marks a current inner source complete.</summary>
    /// <param name="version">The inner source version.</param>
    private void OnCompleted(int version)
    {
        lock (_gate)
        {
            if (IsDone || version != _version)
            {
                return;
            }

            IsInnerActive = false;
            TryComplete();
        }
    }

    /// <summary>Completes once the outer source and current inner source are done.</summary>
    private void TryComplete()
    {
        if (IsDone || !IsOuterCompleted || IsInnerActive)
        {
            return;
        }

        IsDone = true;
        Observer.OnCompleted();
    }
}
