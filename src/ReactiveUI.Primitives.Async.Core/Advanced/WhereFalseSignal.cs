// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Fuses <c>source.Where(static value =&gt; !value)</c> into a single observer layer.</summary>
/// <param name="source">The boolean source observable.</param>
[System.Diagnostics.DebuggerDisplay("WhereFalseSignal")]
public sealed class WhereFalseSignal(IObservableAsync<bool> source) : IObservableAsync<bool>
{
    /// <summary>Creates or reduces a <see langword="false"/>-filtering sequence over <paramref name="source"/>.</summary>
    /// <param name="source">The boolean source observable.</param>
    /// <returns>An observable sequence emitting only <see langword="false"/> values.</returns>
    public static IObservableAsync<bool> Create(IObservableAsync<bool> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source is SignalAsync.EmptySignalAsync<bool> or SignalAsync.NeverSignalAsync<bool> or WhereFalseSignal
            ? source
            : new WhereFalseSignal(source);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<bool>.SubscribeAsync(
        IObserverAsync<bool> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new WhereFalseWitness(observer, cancellationToken),
            observer,
            cancellationToken);
}
