// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits whether the source produced a value equal to the one sought.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ContainsSignal: Source = {_source}")]
public sealed class ContainsSignal<T> : IRequireCurrentThread<bool>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The value to locate.</summary>
    private readonly T _value;

    /// <summary>The comparer used for equality checks.</summary>
    private readonly IEqualityComparer<T> _comparer;

    /// <summary>Initializes a new instance of the <see cref="ContainsSignal{T}"/> class using the default equality comparer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The value to locate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public ContainsSignal(IObservable<T> source, T value)
        : this(source, value, EqualityComparer<T>.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ContainsSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">The comparer used for equality checks, or <see langword="null"/> for the default.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public ContainsSignal(IObservable<T> source, T value, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
        _value = value;
        _comparer = comparer ?? EqualityComparer<T>.Default;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is RangeSignal range && typeof(T) == typeof(int))
        {
            EmitContainsRange(range, _value, _comparer, observer);
            return EmptyDisposable.Instance;
        }

        return !IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired
            ? SubscribeCore(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (Self: this, observer),
                static s => s.Self.SubscribeCore(s.observer));
    }

    /// <summary>Evaluates contains directly over a range source and emits the result.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">The comparer used for equality checks.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitContainsRange(
        RangeSignal range,
        T value,
        IEqualityComparer<T> comparer,
        IObserver<bool> observer)
    {
        try
        {
            if (ReferenceEquals(comparer, EqualityComparer<T>.Default))
            {
                var target = Unsafe.As<T, int>(ref value);
                var offset = (long)target - range.Start;
                observer.OnNext(offset >= 0 && offset < range.Count);
                observer.OnCompleted();
                return;
            }

            for (var i = 0; i < range.Count; i++)
            {
                if (!comparer.Equals((T)(object)(range.Start + i), value))
                {
                    continue;
                }

                observer.OnNext(true);
                observer.OnCompleted();
                return;
            }

            observer.OnNext(false);
            observer.OnCompleted();
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            observer.OnError(error);
        }
    }

    /// <summary>Subscribes the contains sink to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink that owns the upstream subscription.</returns>
    private ContainsWitness<T> SubscribeCore(IObserver<bool> observer)
    {
        ContainsWitness<T> sink = new(observer, _value, _comparer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
