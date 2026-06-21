// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif
/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
/// <remarks>The SignalAsync class contains static methods that extend the functionality of asynchronous
/// observables, enabling advanced composition and control over asynchronous data streams. These methods are intended
/// for use with types that implement asynchronous observer patterns.</remarks>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Scheduler-yielding operators for an observable source sequence.</summary>
    /// <param name = "this">The source observable sequence to yield from.</param>
    /// <typeparam name = "T">The type of the elements in the observable sequence.</typeparam>
    extension<T>(IObservableAsync<T> @this)
    {
        /// <summary>
        /// Returns an observable sequence that yields control to the current thread's scheduler before emitting items from
        /// the source sequence.
        /// </summary>
        /// <remarks>This method can be used to ensure that the source sequence's emissions are scheduled
        /// asynchronously, which may help avoid stack overflows or improve responsiveness in certain scenarios.</remarks>
        /// <returns>An observable sequence that emits the same elements as the source, but yields control to the scheduler before
        /// each emission.</returns>
        public IObservableAsync<T> Yield()
        {
            ArgumentExceptionHelper.ThrowIfNull(@this);
            return new YieldSignal<T>(@this);
        }
    }

    /// <summary>An observable that yields control to the current scheduler before forwarding source emissions.</summary>
    /// <typeparam name = "T">The type of elements in the observable sequence.</typeparam>
    /// <param name = "source">The source observable to yield from.</param>
    internal sealed class YieldSignal<T>(IObservableAsync<T> source) : IObservableAsync<T>
    {
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(IObserverAsync<T> observer, CancellationToken cancellationToken)
        {
            var currentContext = AsyncContext.GetCurrent();
            return source.SubscribeAsync(new ContextSwitchSignalAsync<T>.ContextSwitchWitness(observer, currentContext, true), cancellationToken);
        }
    }
}
