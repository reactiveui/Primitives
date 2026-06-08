// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Emits the latest value from the source sequence or a default value if no value has been emitted.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="defaultValue">The value to emit initially.</param>
internal sealed class LatestOrDefaultObservable<T>(
    IObservable<T> source,
    T defaultValue) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new LatestOrDefaultSink(observer, defaultValue);
        sink.Initialize();
        return source.Subscribe(sink);
    }

    /// <summary>Sink that implements the latest or default logic.</summary>
    /// <param name="downstream">The observer to forward elements to.</param>
    /// <param name="defaultValue">The value to emit initially.</param>
    private sealed class LatestOrDefaultSink(IObserver<T> downstream, T defaultValue) : IObserver<T>
    {
        /// <summary>The last value emitted.</summary>
        private T? _last = defaultValue;

        /// <summary>Whether any value has been emitted yet.</summary>
        private bool _hasEmitted;

        /// <summary>Initializes the sink by emitting the default value.</summary>
        public void Initialize()
        {
            downstream.OnNext(_last!);
            _hasEmitted = true;
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (_hasEmitted && EqualityComparer<T>.Default.Equals(value, _last!))
            {
                return;
            }

            _last = value;
            _hasEmitted = true;
            downstream.OnNext(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
