// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer for enumerable <c>SelectMany</c>.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class SelectManyEnumerableWitness<TSource, TResult> : IObserver<TSource>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer;

    /// <summary>The enumerable projection.</summary>
    private readonly Func<TSource, IEnumerable<TResult>> _selector;

    /// <summary>The upstream subscription.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="SelectManyEnumerableWitness{TSource, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The enumerable projection.</param>
    public SelectManyEnumerableWitness(IObserver<TResult> observer, Func<TSource, IEnumerable<TResult>> selector)
    {
        _observer = observer;
        _selector = selector;
    }

    /// <inheritdoc/>
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, _subscription);

    /// <inheritdoc/>
    public void OnCompleted() => WitnessLifetime.Complete(ref _stopped, _subscription, _observer);

    /// <inheritdoc/>
    public void OnError(Exception error) => WitnessLifetime.Error(ref _stopped, _subscription, _observer, error);

    /// <inheritdoc/>
    public void OnNext(TSource value)
    {
        if (WitnessLifetime.IsStopped(ref _stopped))
        {
            return;
        }

        try
        {
            var values = _selector(value);
            if (values is null)
            {
                WitnessLifetime.Error(
                    ref _stopped,
                    _subscription,
                    _observer,
                    new InvalidOperationException("SelectMany selector returned null."));
                return;
            }

            foreach (var result in values)
            {
                if (WitnessLifetime.IsStopped(ref _stopped))
                {
                    return;
                }

                _observer.OnNext(result);
            }
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            WitnessLifetime.Error(ref _stopped, _subscription, _observer, error);
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, _subscription, subscription);
}
