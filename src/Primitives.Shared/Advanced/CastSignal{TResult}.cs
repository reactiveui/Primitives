// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observable sink that casts object values to the requested result type.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class CastSignal<TResult> : IRequireCurrentThread<TResult>
{
    /// <summary>Initializes a new instance of the <see cref="CastSignal{TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    public CastSignal(IObservable<object?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<object?> Source { get; }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        Source is IRequireCurrentThread<object?> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CastWitness<TResult> castObserver = new(observer);
        castObserver.SetSubscription(Source.Subscribe(castObserver));
        return castObserver;
    }
}
