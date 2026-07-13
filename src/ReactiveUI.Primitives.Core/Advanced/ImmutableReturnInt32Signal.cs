// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Represents the ImmutableReturnInt32Signal class.</summary>
public sealed class ImmutableReturnInt32Signal : IRequireCurrentThread<int>, IInlineSignal<int>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private const int MinCachedValue = -1;

    /// <summary>Stores state for the signal implementation.</summary>
    private const int MaxCachedValue = 9;

    /// <summary>Executes the new operation.</summary>
    /// <returns>The result.</returns>
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

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly int _x;

    /// <summary>Initializes a new instance of the <see cref="ImmutableReturnInt32Signal"/> class.</summary>
    /// <param name="x">The x value.</param>
    public ImmutableReturnInt32Signal(int x) => _x = x;

    /// <summary>Executes the GetInt32Signals operation.</summary>
    /// <param name="x">The x value.</param>
    /// <returns>The result.</returns>
    public static IObservable<int> GetInt32Signals(int x) =>
        x is >= MinCachedValue and <= MaxCachedValue
            ? Caches[x - MinCachedValue]
            : new ImmediateReturnSignal<int>(x);

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        observer.OnNext(_x);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<int> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(_x);
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
