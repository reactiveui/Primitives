// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Implementation types for the count-based buffering overloads.</summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated cold signal for <c>Buffer</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The window size.</param>
    /// <param name="skip">The number of elements skipped between windows.</param>
    private sealed class BufferCountSignal<T>(IObservable<T> source, int count, int skip) : IObservable<IList<T>>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The window size.</summary>
        private readonly int _count = count;

        /// <summary>The number of elements skipped between windows.</summary>
        private readonly int _skip = skip;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<IList<T>> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            BufferWitness<T> sink = new(observer, _count, _skip);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }
}
