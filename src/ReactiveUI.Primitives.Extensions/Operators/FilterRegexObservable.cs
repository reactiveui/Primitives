// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.RegularExpressions;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Operator that filters string elements using a <see cref="Regex"/>. Replaces the closure-based implementation in ReactiveExtensions.Filter.</summary>
/// <param name="source">The source observable emitting strings.</param>
/// <param name="regex">The regex to use for filtering.</param>
internal sealed class FilterRegexObservable(
    IObservable<string> source,
    Regex regex) : IObservable<string>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<string> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(regex);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new FilterRegexObserver(observer, regex));
    }

    /// <summary>Observer that filters strings using regex.</summary>
    /// <param name="downstream">The downstream observer receiving strings that match the regex.</param>
    /// <param name="regex">The regex used for filtering.</param>
    private sealed class FilterRegexObserver(
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
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
