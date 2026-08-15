// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Shared static helpers for task-producing terminal witnesses.</summary>
internal static class TaskTerminalWitnessHelper
{
    /// <summary>Disposes the source subscription.</summary>
    /// <param name="subscription">The subscription slot.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void DisposeSubscription(ref IDisposable? subscription) =>
        Interlocked.Exchange(ref subscription, null)?.Dispose();

    /// <summary>Gets a value indicating whether a witness has stopped.</summary>
    /// <param name="stopped">The stopped flag.</param>
    /// <returns><see langword="true"/> when the sink has stopped.</returns>
    internal static bool IsStopped(ref int stopped) => Volatile.Read(ref stopped) != 0;

    /// <summary>Registers cancellation for a witness after construction.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <param name="state">The witness state.</param>
    /// <param name="callback">The cancellation callback.</param>
    /// <param name="registration">The registration slot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal static void RegisterCancellation<TState>(
        TState state,
        Action<object?> callback,
        ref CancellationTokenRegistration registration,
        CancellationToken cancellationToken)
        where TState : class
    {
        if (!cancellationToken.CanBeCanceled)
        {
            return;
        }

        registration = cancellationToken.UnsafeRegister(callback, state);
    }

    /// <summary>Assigns the source subscription.</summary>
    /// <param name="subscription">The subscription slot.</param>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="value">The subscription to assign.</param>
    internal static void SetSubscription(ref IDisposable? subscription, ref int stopped, IDisposable value)
    {
        ArgumentExceptionHelper.ThrowIfNull(value);

        if (Interlocked.CompareExchange(ref subscription, value, null) is not null)
        {
            value.Dispose();
            return;
        }

        if (Volatile.Read(ref stopped) == 0)
        {
            return;
        }

        DisposeSubscription(ref subscription);
    }

    /// <summary>Attempts to mark a witness stopped.</summary>
    /// <param name="stopped">The stopped flag.</param>
    /// <returns><see langword="true"/> when this caller won the stop race.</returns>
    internal static bool TryStop(ref int stopped) => Interlocked.Exchange(ref stopped, 1) == 0;
}
