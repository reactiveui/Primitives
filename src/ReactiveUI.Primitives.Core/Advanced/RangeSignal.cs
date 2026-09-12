// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Signal that emits a contiguous run of integers, synchronously inside <c>Subscribe</c>.</summary>
[System.Diagnostics.DebuggerDisplay("RangeSignal: Start = {Start}, Count = {Count}")]
public sealed class RangeSignal : IRequireCurrentThread<int>, IInlineSignal<int>
{
    /// <summary>Initializes a new instance of the <see cref="RangeSignal"/> class.</summary>
    /// <param name="start">The first value emitted.</param>
    /// <param name="count">The number of values emitted.</param>
    public RangeSignal(int start, int count)
    {
        Start = start;
        Count = count;
    }

    /// <summary>Gets the first value emitted by the range.</summary>
    public int Start { get; }

    /// <summary>Gets the number of values emitted by the range.</summary>
    public int Count { get; }

    /// <summary>Indicates whether subscription has to happen on the calling thread.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Emits the whole range to <paramref name="observer"/> and completes it before returning.</summary>
    /// <param name="observer">The observer to notify.</param>
    /// <returns>An empty disposable; the range has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        for (var i = 0; i < Count; i++)
        {
            observer.OnNext(Start + i);
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits the whole range to <paramref name="onNext"/>, then invokes <paramref name="onCompleted"/>.</summary>
    /// <param name="onNext">Invoked for each value in the range.</param>
    /// <param name="onError">Never invoked.</param>
    /// <param name="onCompleted">Invoked after the last value.</param>
    /// <returns>An empty disposable; the range has finished by the time this returns.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="onNext"/> is <see langword="null"/>.</exception>
    public IDisposable Subscribe(Action<int> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        for (var i = 0; i < Count; i++)
        {
            onNext(Start + i);
        }

        onCompleted();
        return EmptyDisposable.Instance;
    }
}
