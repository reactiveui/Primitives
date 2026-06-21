// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Operator that buffers characters until a start and end delimiter are found.</summary>
/// <param name="source">The source observable of characters.</param>
/// <param name="startsWith">The starting delimiter.</param>
/// <param name="endsWith">The ending delimiter.</param>
public sealed class BufferUntilObservable(
    IObservable<char> source,
    char startsWith,
    char endsWith) : IObservable<string>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<string> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);
        return source.Subscribe(new BufferUntilWitness(observer, startsWith, endsWith));
    }

    /// <summary>Observer that buffers characters until delimiters are matched.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="startsWith">Start char.</param>
    /// <param name="endsWith">End char.</param>
    private sealed class BufferUntilWitness(
        IObserver<string> downstream,
        char startsWith,
        char endsWith) : IObserver<char>
    {
        /// <summary>The string builder.</summary>
        private readonly StringBuilder _sb = new();

        /// <summary>Whether the start delimiter has been found.</summary>
        private bool _startFound;

        /// <inheritdoc/>
        public void OnNext(char value)
        {
            if (!_startFound && value != startsWith)
            {
                return;
            }

            _startFound = true;
            _ = _sb.Append(value);

            if (value != endsWith)
            {
                return;
            }

            downstream.OnNext(_sb.ToString());
            _startFound = false;
            _ = _sb.Clear();
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (_startFound && _sb.Length > 0)
            {
                downstream.OnNext(_sb.ToString());
            }

            downstream.OnCompleted();
        }
    }
}
