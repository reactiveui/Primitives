// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Fuses <c>source.Select(static value =&gt; !value)</c> into a single observer layer.</summary>
/// <param name="source">The boolean source observable.</param>
[System.Diagnostics.DebuggerDisplay("NotSignal: Source = {Source}")]
public sealed class NotSignal(IObservableAsync<bool> source) : IObservableAsync<bool>
{
    /// <summary>Gets the underlying boolean source sequence.</summary>
    internal IObservableAsync<bool> Source => source;

    /// <summary>Creates or reduces a boolean negation sequence over <paramref name="source"/>.</summary>
    /// <param name="source">The boolean source observable.</param>
    /// <returns>An observable sequence of negated boolean values.</returns>
    public static IObservableAsync<bool> Create(IObservableAsync<bool> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source switch
        {
            NotSignal inner => inner.Source,
            SignalAsync.EmptySignalAsync<bool> or SignalAsync.NeverSignalAsync<bool> => source,
            _ => new NotSignal(source),
        };
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<bool>.SubscribeAsync(
        IObserverAsync<bool> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new NotWitness(observer, cancellationToken),
            observer,
            cancellationToken);
}
