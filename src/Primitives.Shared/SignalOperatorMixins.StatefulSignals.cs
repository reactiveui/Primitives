// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary><see cref="IObservable{T}"/> implementations for the stateful single-source operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Dedicated signal for <c>Take</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The maximum number of values to forward.</param>
    private sealed class TakeSignal<T>(IObservable<T> source, int count) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The maximum number of values to forward.</summary>
        private readonly int _count = count;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (_count == 0)
            {
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            if (!CurrentThreadRequirement.IsRequired(_source) || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return SubscribeCore(observer);
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (Self: this, subscription, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.Self.SubscribeCore(s.observer));
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }

        /// <summary>Subscribes the counting sink to the source.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The sink that owns the upstream subscription.</returns>
        private TakeWitness<T> SubscribeCore(IObserver<T> observer)
        {
            TakeWitness<T> sink = new(observer, _count);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>TakeUntil</c>, holding both the source and the stop arm.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <typeparam name="TOther">The cancellation value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="other">The observable that stops the source when it emits.</param>
    private sealed class TakeUntilSignal<T, TOther>(IObservable<T> source, IObservable<TOther> other) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The observable that stops the source when it emits.</summary>
        private readonly IObservable<TOther> _other = other;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if ((!CurrentThreadRequirement.IsRequired(_source) && !CurrentThreadRequirement.IsRequired(_other))
                || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return SubscribeCore(observer);
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (Self: this, subscription, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.Self.SubscribeCore(s.observer));
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }

        /// <summary>Subscribes the stop arm and, unless it has fired, the source.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The coordinator that owns both subscriptions.</returns>
        private TakeUntilCoordinator SubscribeCore(IObserver<T> observer)
        {
            TakeUntilCoordinator coordinator = new(observer);
            coordinator.Add(_other.Subscribe(new TakeUntilOtherWitness(coordinator)));
            if (coordinator.IsStopped)
            {
                return coordinator;
            }

            coordinator.Add(_source.Subscribe(new TakeUntilSourceWitness(coordinator)));
            return coordinator;
        }

        /// <summary>Coordinates serialized observer callbacks and subscription lifetime.</summary>
        /// <remarks>
        /// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while the observer runs; the
        /// first terminal notification wins and nothing follows it.
        /// </remarks>
        private sealed class TakeUntilCoordinator : IDisposable
        {
            /// <summary>The downstream observer.</summary>
            private readonly IObserver<T> _observer;

            /// <summary>Tracks the source and cancellation subscriptions.</summary>
            private readonly MultipleDisposable _subscriptions = [];

            /// <summary>Serializes downstream deliveries.</summary>
            private SerializedDelivery<T> _delivery = new();

            /// <summary>Indicates whether the sequence has stopped.</summary>
            private int _stopped;

            /// <summary>Initializes a new instance of the <see cref="TakeUntilCoordinator"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            internal TakeUntilCoordinator(IObserver<T> observer) => _observer = observer;

            /// <summary>Gets a value indicating whether the sequence has stopped.</summary>
            internal bool IsStopped => Volatile.Read(ref _stopped) != 0;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => _subscriptions.Dispose();

            /// <summary>Adds a subscription to the coordinator lifetime.</summary>
            /// <param name="subscription">The subscription to add.</param>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Add(IDisposable subscription) => _subscriptions.Add(subscription);

            /// <summary>Forwards a source value when the sequence has not stopped.</summary>
            /// <param name="value">The source value.</param>
            internal void Next(T value)
            {
                if (IsStopped)
                {
                    return;
                }

                _delivery.OnNext(_observer, value, new PendingDrain(this));
            }

            /// <summary>Completes the downstream observer once, after any value being delivered, and disposes all subscriptions.</summary>
            internal void Complete()
            {
                if (Interlocked.Exchange(ref _stopped, 1) != 0)
                {
                    return;
                }

                _delivery.OnCompleted(new PendingDrain(this));
                _subscriptions.Dispose();
            }

            /// <summary>Sends an error to the downstream observer once, after any value being delivered, and disposes all subscriptions.</summary>
            /// <param name="exception">The exception to forward.</param>
            internal void Error(Exception exception)
            {
                if (Interlocked.Exchange(ref _stopped, 1) != 0)
                {
                    return;
                }

                _delivery.OnError(exception, new PendingDrain(this));
                _subscriptions.Dispose();
            }

            /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
            /// <param name="Owner">The coordinator.</param>
            private readonly record struct PendingDrain(TakeUntilCoordinator Owner) : IDrainTarget
            {
                /// <inheritdoc/>
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
            }
        }

        /// <summary>Observes the source stream and routes its notifications through the coordinator.</summary>
        private sealed class TakeUntilSourceWitness : IObserver<T>
        {
            /// <summary>The owning coordinator.</summary>
            private readonly TakeUntilCoordinator _coordinator;

            /// <summary>Initializes a new instance of the <see cref="TakeUntilSourceWitness"/> class.</summary>
            /// <param name="coordinator">The owning coordinator.</param>
            internal TakeUntilSourceWitness(TakeUntilCoordinator coordinator) => _coordinator = coordinator;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(T value) => _coordinator.Next(value);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => _coordinator.Error(error);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnCompleted() => _coordinator.Complete();
        }

        /// <summary>Observes the cancellation stream; its first value or error stops the source.</summary>
        private sealed class TakeUntilOtherWitness : IObserver<TOther>
        {
            /// <summary>The owning coordinator.</summary>
            private readonly TakeUntilCoordinator _coordinator;

            /// <summary>Initializes a new instance of the <see cref="TakeUntilOtherWitness"/> class.</summary>
            /// <param name="coordinator">The owning coordinator.</param>
            internal TakeUntilOtherWitness(TakeUntilCoordinator coordinator) => _coordinator = coordinator;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnNext(TOther value) => _coordinator.Complete();

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnError(Exception error) => _coordinator.Error(error);

            /// <inheritdoc/>
            public void OnCompleted()
            {
                // Completion of the cancellation stream without a value does not stop the source.
            }
        }
    }

    /// <summary>An observable that emits a single <see cref="RxVoid"/> value when its cancellation token is canceled; the stop source for <c>TakeUntil(CancellationToken)</c>.</summary>
    private sealed class CancellationSignal : IObservable<RxVoid>
    {
        /// <summary>The token whose cancellation triggers the emission.</summary>
        private readonly CancellationToken _cancellationToken;

        /// <summary>Initializes a new instance of the <see cref="CancellationSignal"/> class.</summary>
        /// <param name="cancellationToken">The token whose cancellation triggers the emission.</param>
        internal CancellationSignal(CancellationToken cancellationToken) => _cancellationToken = cancellationToken;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<RxVoid> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return _cancellationToken.UnsafeRegister(
                static state => ((IObserver<RxVoid>)state!).OnNext(RxVoid.Default),
                observer);
        }
    }

    /// <summary>Dedicated signal for <c>Skip</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The number of leading values to drop.</param>
    private sealed class SkipSignal<T>(IObservable<T> source, int count) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The number of leading values to drop.</summary>
        private readonly int _count = count;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            SkipWitness<T> sink = new(observer, _count);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>Distinct</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="comparer">The comparer used to identify duplicates.</param>
    private sealed class DistinctSignal<T>(IObservable<T> source, IEqualityComparer<T>? comparer) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The comparer used to identify duplicates.</summary>
        private readonly IEqualityComparer<T>? _comparer = comparer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            DistinctWitness<T> sink = new(observer, CreateSeen());
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }

        /// <summary>Creates the duplicate-tracking set, pre-sized when the source has a known element count.</summary>
        /// <returns>The set that tracks the values seen so far.</returns>
        private HashSet<T> CreateSeen() =>
#if NET8_0_OR_GREATER
            (_source is RangeSignal range ? range.Count : 0) switch
            {
                var capacity when capacity > 0 => [with(capacity, _comparer)],
                _ when _comparer is null => [],
                _ => [with(_comparer)],
            };
#else
            [with(_comparer)];
#endif
    }

    /// <summary>Dedicated signal for <c>UniqueBy</c> (adjacent distinct by key).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="keySelector">The key projection.</param>
    /// <param name="comparer">The comparer used to compare adjacent keys.</param>
    private sealed class UniqueBySignal<T, TKey>(IObservable<T> source, Func<T, TKey> keySelector, IEqualityComparer<TKey> comparer) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The key projection.</summary>
        private readonly Func<T, TKey> _keySelector = keySelector;

        /// <summary>The comparer used to compare adjacent keys.</summary>
        private readonly IEqualityComparer<TKey> _comparer = comparer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            UniqueByWitness<T, TKey> sink = new(observer, _keySelector, _comparer);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>TakeWhile</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate that determines whether to keep taking values.</param>
    private sealed class TakeWhileSignal<T>(IObservable<T> source, Func<T, bool> predicate) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The predicate that determines whether to keep taking values.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            if (!CurrentThreadRequirement.IsRequired(_source) || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return SubscribeCore(observer);
            }

            SingleDisposable subscription = new();
            _ = Sequencer.CurrentThread.Schedule(
                (Self: this, subscription, observer),
                static (_, s) =>
                {
                    s.subscription.Create(s.Self.SubscribeCore(s.observer));
                    return EmptyDisposable.Instance;
                });
            return subscription;
        }

        /// <summary>Subscribes the predicate sink to the source.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The sink that owns the upstream subscription.</returns>
        private TakeWhileWitness<T> SubscribeCore(IObserver<T> observer)
        {
            TakeWhileWitness<T> sink = new(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Dedicated signal for <c>SkipWhile</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate that determines whether to keep skipping values.</param>
    private sealed class SkipWhileSignal<T>(IObservable<T> source, Func<T, bool> predicate) : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source = source;

        /// <summary>The predicate that determines whether to keep skipping values.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            SkipWhileWitness<T> sink = new(observer, _predicate);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }
}
