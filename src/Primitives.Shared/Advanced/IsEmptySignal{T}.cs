// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that emits whether the source completed without values.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class IsEmptySignal<T> : IRequireCurrentThread<bool>
{
    /// <summary>Initializes a new instance of the <see cref="IsEmptySignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    public IsEmptySignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        Source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (Source is ImmutableEmptySignal<T>)
        {
            observer.OnNext(true);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        if (Source is RangeSignal { Count: > 0 })
        {
            observer.OnNext(false);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        IsEmptyWitness<T> isEmptyObserver = new(observer);
        isEmptyObserver.SetSubscription(Source.Subscribe(isEmptyObserver));
        return isEmptyObserver;
    }
}
