// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Per-subscription witness that suppresses duplicate terminal notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Generation = {_generation}, Terminated = {_terminated}")]
public sealed class RepeatSourceWitness<T> : IObserver<T>
{
    /// <summary>The owning repeat coordinator.</summary>
    private readonly RepeatSourceCoordinator<T> _parent;

    /// <summary>The source subscription generation.</summary>
    private readonly int _generation;

    /// <summary>Tracks whether this source subscription has already terminated.</summary>
    private int _terminated;

    /// <summary>Initializes a new instance of the <see cref="RepeatSourceWitness{T}"/> class.</summary>
    /// <param name="parent">The owning repeat coordinator.</param>
    /// <param name="generation">The source subscription generation.</param>
    /// <exception cref="ArgumentNullException"><paramref name="parent"/> is <see langword="null"/>.</exception>
    public RepeatSourceWitness(RepeatSourceCoordinator<T> parent, int generation)
    {
        ArgumentExceptionHelper.ThrowIfNull(parent);

        _parent = parent;
        _generation = generation;
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _terminated, 1) != 0)
        {
            return;
        }

        _parent.OnCompleted(_generation);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (Interlocked.Exchange(ref _terminated, 1) != 0)
        {
            return;
        }

        _parent.OnError(_generation, error);
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _terminated) != 0)
        {
            return;
        }

        _parent.OnNext(_generation, value);
    }
}
