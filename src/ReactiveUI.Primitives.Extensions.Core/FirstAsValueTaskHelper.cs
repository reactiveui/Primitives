// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Sources;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// <see cref="ValueTask{T}"/>-returning counterpart to <see cref="FirstAsTaskHelper"/>. Backs the
/// <c>ToHotValueTask</c> extension with a pooled <see cref="IValueTaskSource{T}"/> implementation
/// so steady-state callers pay zero allocations after the pool warms up.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[SuppressMessage(
    "Major Code Smell",
    "S2743:Static fields should not be used in generic types",
    Justification = "The pooled witness stores T-specific value task source state, so a per-type pool is intentional.")]
public static class FirstAsValueTaskHelper<T>
{
    /// <summary>Single-slot pool. <c>null</c> when the previous instance is in flight.</summary>
    private static PooledFirstWitness? _pooled;

    /// <summary>Subscribes once and resolves a <see cref="ValueTask{T}"/> with the first value.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>A value task that completes with the first value, faults on error, or faults on empty completion.</returns>
    public static ValueTask<T> FirstAsValueTask(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        var inst = Interlocked.Exchange(ref _pooled, null) ?? new PooledFirstWitness();
        return inst.Begin(source);
    }

    /// <summary>Pooled combined <see cref="IValueTaskSource{T}"/> + <see cref="IObserver{T}"/>.</summary>
    private sealed class PooledFirstWitness : IValueTaskSource<T>, IObserver<T>
    {
        /// <summary>The reset-able backing store for the <see cref="ValueTask{T}"/> machinery.</summary>
        private ManualResetValueTaskSourceCore<T> _core = new() { RunContinuationsAsynchronously = true };

        /// <summary>Latches to <c>1</c> once the source has been settled so subsequent callbacks no-op.</summary>
        private int _settled;

        /// <summary>The upstream subscription, retained so <see cref="OnNext"/> can cancel it on first match.</summary>
        private IDisposable? _subscription;

        /// <summary>Begins a fresh capture cycle: resets the core, subscribes the source, returns the task.</summary>
        /// <param name="source">The source observable to subscribe to.</param>
        /// <returns>The value task observers consume to receive the first value.</returns>
        public ValueTask<T> Begin(IObservable<T> source)
        {
            _core.Reset();
            _settled = 0;
            _subscription = source.Subscribe(this);
            return new(this, _core.Version);
        }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            _subscription?.Dispose();
            _core.SetResult(value);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            _core.SetException(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            _core.SetException(new InvalidOperationException("Sequence contains no elements."));
        }

        /// <inheritdoc/>
        ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token) => _core.GetStatus(token);

        /// <inheritdoc/>
        void IValueTaskSource<T>.OnCompleted(
            Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);

        /// <inheritdoc/>
        T IValueTaskSource<T>.GetResult(short token)
        {
            try
            {
                return _core.GetResult(token);
            }
            finally
            {
                _subscription = null;
                Interlocked.CompareExchange(ref _pooled, this, null);
            }
        }
    }
}
