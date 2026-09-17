// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that emits a single <see cref="int"/> and completes synchronously inside <c>Subscribe</c>.</summary>
[System.Diagnostics.DebuggerDisplay("ImmutableReturnInt32Signal: Value = {_x}")]
public sealed class ImmutableReturnInt32Signal : IRequireCurrentThread<int>, IInlineSignal<int>
{
    /// <summary>The lowest value with a cached instance.</summary>
    private const int MinCachedValue = -1;

    /// <summary>The highest value with a cached instance.</summary>
    private const int MaxCachedValue = 9;

    /// <summary>The instances cached for values from <see cref="MinCachedValue"/> to <see cref="MaxCachedValue"/>.</summary>
    private static readonly ImmutableReturnInt32Signal[] Caches =
    [
        new(-1),
        new(0),
        new(1),
        new(2),
        new(3),
        new(4),
        new(5),
        new(6),
        new(7),
        new(8),
        new(9)
    ];

    /// <summary>The value emitted to every subscriber.</summary>
    private readonly int _x;

    /// <summary>Initializes a new instance of the <see cref="ImmutableReturnInt32Signal"/> class.</summary>
    /// <param name="x">The value to emit.</param>
    public ImmutableReturnInt32Signal(int x) => _x = x;

    /// <summary>Returns a signal emitting <paramref name="x"/>, reusing a cached instance for small values.</summary>
    /// <param name="x">The value to emit.</param>
    /// <returns>A signal that emits <paramref name="x"/> and completes.</returns>
    public static IObservable<int> GetInt32Signals(int x) =>
        x is >= MinCachedValue and <= MaxCachedValue
            ? Caches[x - MinCachedValue]
            : new ImmediateReturnSignal<int>(x);

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Emits the value to <paramref name="observer"/> and completes it before returning.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        observer.OnNext(_x);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes <paramref name="onNext"/> with the value, then <paramref name="onCompleted"/>.</summary>
    /// <param name="onNext">Invoked with the value.</param>
    /// <param name="onError">Never invoked.</param>
    /// <param name="onCompleted">Invoked after the value.</param>
    /// <returns>An empty disposable; the signal has finished by the time this returns.</returns>
    public IDisposable Subscribe(Action<int> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(_x);
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
