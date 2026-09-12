// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Forwards the source strings that <paramref name="regex"/> matches, dropping non-matches and nulls. An exception
/// raised while matching, such as a regex timeout, terminates the sequence.
/// </summary>
/// <param name="source">The source observable emitting strings.</param>
/// <param name="regex">The regex to use for filtering.</param>
public sealed class FilterRegexObservable(
    IObservable<string> source,
    Regex regex) : IObservable<string>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<string> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(regex);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new FilterRegexWitness(observer, regex));
    }

    /// <summary>Observer that forwards matching strings and turns a failure raised by the match into an error.</summary>
    /// <param name="downstream">The downstream observer receiving strings that match the regex.</param>
    /// <param name="regex">The regex used for filtering.</param>
    private sealed class FilterRegexWitness(
        IObserver<string> downstream,
        Regex regex) : IObserver<string>
    {
        /// <inheritdoc/>
        public void OnNext(string value)
        {
            try
            {
                if (value is not null && regex.IsMatch(value))
                {
                    downstream.OnNext(value);
                }
            }
            catch (Exception ex)
            {
                downstream.OnError(ex);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => downstream.OnCompleted();
    }
}
