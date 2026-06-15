// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that forwards only values assignable to <typeparamref name="TResult"/>.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class KeepTypeWitness<TResult> : IObserver<object?>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="KeepTypeWitness{TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public KeepTypeWitness(IObserver<TResult> observer) => _observer = observer;

    /// <inheritdoc/>
    public void OnNext(object? value)
    {
        if (value is not TResult result)
        {
            return;
        }

        try
        {
            _observer.OnNext(result);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    public void OnCompleted() => SinkTerminal.Complete(_observer, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
