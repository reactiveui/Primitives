// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Fused <c>Map(selector).KeepNotNull()</c> signal.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The non-null reference result type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="selector">The nullable projection function.</param>
[System.Diagnostics.DebuggerDisplay("MapKeepNotNullSignal: Source = {_source}")]
public sealed class MapKeepNotNullSignal<TSource, TResult>(
    IObservable<TSource> source,
    Func<TSource, TResult?> selector) : IRequireCurrentThread<TResult>, IInlineSignal<TResult>
    where TResult : class
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The nullable projection function.</summary>
    private readonly Func<TSource, TResult?> _selector = selector;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() =>
        DirectSequenceEmission.RequiresCurrentThread(_source);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (DirectSequenceEmission.TrySubscribeFilteredProjection(
            _source,
            _selector,
            static (item, sel) => Unsafe.As<Func<int, TResult?>>(sel)(item) is { } nonNull ? new(true, nonNull) : default,
            static (item, sel) => sel(item) is { } nonNull ? new(true, nonNull) : default,
            observer))
        {
            return EmptyDisposable.Instance;
        }

        MapKeepNotNullWitness<TSource, TResult> sink = new(observer, _selector);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<TResult> onNext, Action<Exception> onError, Action onCompleted)
    {
        _ = DirectSequenceEmission.ValidateCallbacks(onNext, onError, onCompleted);
        return Subscribe(new EmptyWitness<TResult>(onNext, onError, onCompleted));
    }
}
