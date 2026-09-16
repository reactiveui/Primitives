// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that forwards only the source values of a given type.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("KeepTypeSignal: Source = {_source}")]
public sealed class KeepTypeSignal<TResult> : IObservable<TResult>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<object?> _source;

    /// <summary>Initializes a new instance of the <see cref="KeepTypeSignal{TResult}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public KeepTypeSignal(IObservable<object?> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        KeepTypeWitness<TResult> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
