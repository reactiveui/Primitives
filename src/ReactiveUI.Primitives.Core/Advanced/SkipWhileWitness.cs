// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that drops leading values while the predicate holds, then mirrors the source.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="predicate">The predicate that determines whether to keep skipping values.</param>
public sealed class SkipWhileWitness<T>(IObserver<T> observer, Func<T, bool> predicate) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>The predicate that determines whether to keep skipping values.</summary>
    private readonly Func<T, bool> _predicate = predicate;

    /// <summary>A value indicating whether the skipping phase is still active.</summary>
    private bool _skipping = true;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (_skipping && _predicate(value))
        {
            return;
        }

        _skipping = false;
        try
        {
            _observer.OnNext(value);
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
