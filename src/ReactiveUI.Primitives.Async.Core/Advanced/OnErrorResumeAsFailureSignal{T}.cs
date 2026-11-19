// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that converts resumable errors from the source into failure completion results.</summary>
/// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("OnErrorResumeAsFailureSignal: Source = {_source}")]
public sealed class OnErrorResumeAsFailureSignal<T> : IObservableAsync<T>
{
    /// <summary>The source observable to monitor for errors.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>Initializes a new instance of the <see cref="OnErrorResumeAsFailureSignal{T}"/> class.</summary>
    /// <param name="source">The source observable to monitor for errors.</param>
    public OnErrorResumeAsFailureSignal(IObservableAsync<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new OnErrorResumeAsFailureWitness<T>(observer),
            observer,
            cancellationToken);
}
