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
public sealed class SelectManyEnumerableObserver<TSource, TResult> : IObserver<TSource>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer;

    /// <summary>The enumerable projection.</summary>
    private readonly Func<TSource, IEnumerable<TResult>> _selector;

    /// <summary>The upstream subscription.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="SelectManyEnumerableObserver{TSource, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The enumerable projection.</param>
    public SelectManyEnumerableObserver(IObserver<TResult> observer, Func<TSource, IEnumerable<TResult>> selector)
    {
        _observer = observer;
        _selector = selector;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Interlocked.Exchange(ref _stopped, 1);
        _subscription.Dispose();
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            _subscription.Dispose();
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => StopWithError(error);

    /// <inheritdoc/>
    public void OnNext(TSource value)
    {
        if (Volatile.Read(ref _stopped) != 0)
        {
            return;
        }

        try
        {
            var values = _selector(value);
            if (values is null)
            {
                StopWithError(new InvalidOperationException("SelectMany selector returned null."));
                return;
            }

            foreach (var result in values)
            {
                if (Volatile.Read(ref _stopped) != 0)
                {
                    return;
                }

                _observer.OnNext(result);
            }
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            StopWithError(error);
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription)
    {
        _subscription.Create(subscription);
        if (Volatile.Read(ref _stopped) == 0)
        {
            return;
        }

        _subscription.Dispose();
    }

    /// <summary>Forwards an error exactly once and disposes the upstream subscription.</summary>
    /// <param name="error">The terminal error.</param>
    private void StopWithError(Exception error)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        try
        {
            _observer.OnError(error);
        }
        finally
        {
            _subscription.Dispose();
        }
    }
}
