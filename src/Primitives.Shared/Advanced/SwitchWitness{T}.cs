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
        Subscriptions.Add(sources.Subscribe(OnSource, Observer.OnError, OnOuterCompleted));
        return this;
    }

    /// <summary>Switches to a new inner source.</summary>
    /// <param name="source">The new inner source.</param>
    private void OnSource(IObservable<T> source)
    {
        int current;
        lock (_gate)
        {
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
            IsOuterCompleted = true;
        }

        TryComplete();
    }

    /// <summary>Forwards a current inner value.</summary>
    /// <param name="version">The inner source version.</param>
    /// <param name="value">The value to forward.</param>
    private void OnNext(int version, T value)
    {
        if (!IsCurrent(version))
        {
            return;
        }

        Observer.OnNext(value);
    }

    /// <summary>Forwards a current inner error.</summary>
    /// <param name="version">The inner source version.</param>
    /// <param name="error">The error to forward.</param>
    private void OnError(int version, Exception error)
    {
        if (!IsCurrent(version))
        {
            return;
        }

        Observer.OnError(error);
    }

    /// <summary>Marks a current inner source complete.</summary>
    /// <param name="version">The inner source version.</param>
    private void OnCompleted(int version)
    {
        lock (_gate)
        {
            if (version == _version)
            {
                IsInnerActive = false;
            }
        }

        TryComplete();
    }

    /// <summary>Determines whether a version is current.</summary>
    /// <param name="version">The candidate version.</param>
    /// <returns><see langword="true"/> when the version is current.</returns>
    private bool IsCurrent(int version) => version == Volatile.Read(ref _version);

    /// <summary>Completes once the outer source and current inner source are done.</summary>
    private void TryComplete()
    {
        lock (_gate)
        {
            if (IsOuterCompleted && !IsInnerActive)
            {
                Observer.OnCompleted();
            }
        }
    }
}
