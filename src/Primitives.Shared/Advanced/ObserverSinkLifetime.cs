// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Shared terminal and subscription handling for advanced observer sinks.</summary>
internal static class ObserverSinkLifetime
{
    /// <summary>Disposes a sink and its upstream subscription.</summary>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="subscription">The upstream subscription slot.</param>
    public static void Dispose(ref int stopped, SingleReplaceableDisposable subscription)
    {
        Interlocked.Exchange(ref stopped, 1);
        subscription.Dispose();
    }

    /// <summary>Gets a value indicating whether the sink has stopped.</summary>
    /// <param name="stopped">The stopped flag.</param>
    /// <returns><see langword="true"/> when the sink has stopped.</returns>
    public static bool IsStopped(ref int stopped) => Volatile.Read(ref stopped) != 0;

    /// <summary>Assigns the upstream subscription and disposes it when the sink has already stopped.</summary>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="slot">The upstream subscription slot.</param>
    /// <param name="subscription">The upstream subscription.</param>
    public static void SetSubscription(ref int stopped, SingleReplaceableDisposable slot, IDisposable subscription)
    {
        slot.Create(subscription);
        if (Volatile.Read(ref stopped) == 0)
        {
            return;
        }

        slot.Dispose();
    }

    /// <summary>Forwards completion exactly once and disposes the upstream subscription.</summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="subscription">The upstream subscription slot.</param>
    /// <param name="observer">The downstream observer.</param>
    public static void Complete<T>(
        ref int stopped,
        SingleReplaceableDisposable subscription,
        IObserver<T> observer)
    {
        if (Interlocked.Exchange(ref stopped, 1) != 0)
        {
            return;
        }

        try
        {
            observer.OnCompleted();
        }
        finally
        {
            subscription.Dispose();
        }
    }

    /// <summary>Forwards an error exactly once and disposes the upstream subscription.</summary>
    /// <typeparam name="T">The observer value type.</typeparam>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="subscription">The upstream subscription slot.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="terminalError">The terminal error.</param>
    public static void Error<T>(
        ref int stopped,
        SingleReplaceableDisposable subscription,
        IObserver<T> observer,
        Exception terminalError)
    {
        if (Interlocked.Exchange(ref stopped, 1) != 0)
        {
            return;
        }

        using var _ = subscription;
        observer.OnError(terminalError);
    }
}
