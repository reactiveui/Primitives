// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer for enumerable <c>SelectMany</c>.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="selector">The enumerable projection.</param>
[System.Diagnostics.DebuggerDisplay("SelectManyEnumerableWitness: Stopped = {_stopped}, Observer = {_observer}")]
public sealed class SelectManyEnumerableWitness<TSource, TResult>(IObserver<TResult> observer, Func<TSource, IEnumerable<TResult>> selector) : IObserver<TSource>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <summary>The enumerable projection.</summary>
    private readonly Func<TSource, IEnumerable<TResult>> _selector = selector;

    /// <summary>The upstream subscription.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, _subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => WitnessLifetime.Complete(ref _stopped, _subscription, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, _subscription, subscription);
}
