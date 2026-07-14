// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Private helper types for boolean terminal parity operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Predicate all operator implemented without delegate observer wrappers.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate.</param>
    private sealed class AllPredicateSignal<T>(IObservable<T> source, Func<T, bool> predicate) : IRequireCurrentThread<bool>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The predicate.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<bool> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                EmitAllRange(range, _predicate, observer);
                return EmptyDisposable.Instance;
            }

            // The first value the predicate rejects settles this operator, so it must own the source subscription
            // before the source starts producing. A current-thread source drains its trampoline inside its own
            // Subscribe, so on an endless source the sink would never be handed the subscription it needs to stop it.
            if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return SubscribeCore(observer);
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (self: this, subscription, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.self.SubscribeCore(s.observer));
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }

        /// <summary>Evaluates a predicate directly over a range source and emits the all result.</summary>
        /// <param name="range">The range source.</param>
        /// <param name="predicate">The predicate.</param>
        /// <param name="observer">The downstream observer.</param>
        private static void EmitAllRange(RangeSignal range, Func<T, bool> predicate, IObserver<bool> observer)
        {
            try
            {
                var typedPredicate = (Func<int, bool>)(object)predicate;
                for (var i = 0; i < range.Count; i++)
                {
                    if (typedPredicate(range.Start + i))
                    {
                        continue;
                    }

                    observer.OnNext(false);
                    observer.OnCompleted();
                    return;
                }

                observer.OnNext(true);
                observer.OnCompleted();
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }

        /// <summary>Subscribes the predicated all sink to the source.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The sink that owns the upstream subscription.</returns>
        private AllPredicateWitness<T> SubscribeCore(IObserver<bool> observer)
        {
            AllPredicateWitness<T> sink = new(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Contains operator implemented without composing Any and comparer closures.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">The comparer used for equality checks.</param>
    private sealed class ContainsSignal<T>(IObservable<T> source, T value, IEqualityComparer<T> comparer) : IRequireCurrentThread<bool>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The value to locate.</summary>
        private readonly T _value = value;

        /// <summary>The comparer used for equality checks.</summary>
        private readonly IEqualityComparer<T> _comparer = comparer;

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() =>
            _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<bool> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_source is RangeSignal range && typeof(T) == typeof(int))
            {
                EmitContainsRange(range, _value, _comparer, observer);
                return EmptyDisposable.Instance;
            }

            // The value being sought settles this operator the moment it arrives, so it must own the source
            // subscription before the source starts producing. See AllPredicateSignal for the livelock without this.
            if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return SubscribeCore(observer);
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (self: this, subscription, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.self.SubscribeCore(s.observer));
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }

        /// <summary>Evaluates contains directly over a range source and emits the result.</summary>
        /// <param name="range">The range source.</param>
        /// <param name="value">The value to locate.</param>
        /// <param name="comparer">The comparer used for equality checks.</param>
        /// <param name="observer">The downstream observer.</param>
        private static void EmitContainsRange(
            RangeSignal range,
            T value,
            IEqualityComparer<T> comparer,
            IObserver<bool> observer)
        {
            try
            {
                if (ReferenceEquals(comparer, EqualityComparer<T>.Default))
                {
                    var target = (int)(object)value!;
                    var offset = (long)target - range.Start;
                    observer.OnNext(offset >= 0 && offset < range.Count);
                    observer.OnCompleted();
                    return;
                }

                for (var i = 0; i < range.Count; i++)
                {
                    if (!comparer.Equals((T)(object)(range.Start + i), value))
                    {
                        continue;
                    }

                    observer.OnNext(true);
                    observer.OnCompleted();
                    return;
                }

                observer.OnNext(false);
                observer.OnCompleted();
            }
            catch (Exception error)
            {
                observer.OnError(error);
            }
        }

        /// <summary>Subscribes the contains sink to the source.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The sink that owns the upstream subscription.</returns>
        private ContainsWitness<T> SubscribeCore(IObserver<bool> observer)
        {
            ContainsWitness<T> sink = new(observer, _value, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }
}
