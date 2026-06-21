// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Shared lifecycle helpers for witness wrappers that own one cancellation resource.</summary>
internal static class WitnessLifetime
{
    /// <summary>Disposes a sink and its upstream subscription.</summary>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="subscription">The upstream subscription slot.</param>
    public static void Dispose(ref int stopped, SingleReplaceableDisposable subscription)
    {
        _ = Interlocked.Exchange(ref stopped, 1);
        subscription.Dispose();
    }

    /// <summary>Releases the cancellation resource and marks the witness stopped.</summary>
    /// <param name="cancelSlot">The slot that owns the cancellation resource.</param>
    /// <param name="stopped">The stopped flag.</param>
    public static void Dispose(ref IDisposable? cancelSlot, ref int stopped)
    {
        Interlocked.Exchange(ref cancelSlot, null)?.Dispose();
        Volatile.Write(ref stopped, 1);
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

        using var _ = subscription;
        observer.OnCompleted();
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

    /// <summary>Assigns the cancellation resource or disposes it when the witness already stopped.</summary>
    /// <param name="cancelSlot">The slot that owns the cancellation resource.</param>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="cancel">The cancellation resource to assign.</param>
    public static void SetCancel(ref IDisposable? cancelSlot, ref int stopped, IDisposable cancel)
    {
        ArgumentExceptionHelper.ThrowIfNull(cancel);

        if (Interlocked.CompareExchange(ref cancelSlot, cancel, null) is not null)
        {
            cancel.Dispose();
            return;
        }

        if (Volatile.Read(ref stopped) == 0)
        {
            return;
        }

        Interlocked.Exchange(ref cancelSlot, null)?.Dispose();
    }

    /// <summary>Forwards a value when the witness is still active.</summary>
    /// <typeparam name="TOwner">The witness owner type.</typeparam>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="owner">The witness owner.</param>
    /// <param name="value">The value to forward.</param>
    /// <param name="forward">The forwarding action.</param>
    public static void OnNext<TOwner, T>(ref int stopped, TOwner owner, T value, Action<TOwner, T> forward)
        where TOwner : class
    {
        if (Volatile.Read(ref stopped) != 0)
        {
            return;
        }

        forward(owner, value);
    }

    /// <summary>Forwards a terminal error once and then disposes the owner.</summary>
    /// <typeparam name="TOwner">The witness owner type.</typeparam>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="owner">The witness owner.</param>
    /// <param name="error">The error to forward.</param>
    /// <param name="forward">The error forwarding action.</param>
    /// <param name="dispose">The owner disposal action.</param>
    public static void OnError<TOwner>(
        ref int stopped,
        TOwner owner,
        Exception error,
        Action<TOwner, Exception> forward,
        Action<TOwner> dispose)
        where TOwner : class
    {
        if (Interlocked.Exchange(ref stopped, 1) != 0)
        {
            return;
        }

        try
        {
            forward(owner, error);
        }
        finally
        {
            dispose(owner);
        }
    }

    /// <summary>Forwards completion once and then disposes the owner.</summary>
    /// <typeparam name="TOwner">The witness owner type.</typeparam>
    /// <param name="stopped">The stopped flag.</param>
    /// <param name="owner">The witness owner.</param>
    /// <param name="forward">The completion forwarding action.</param>
    /// <param name="dispose">The owner disposal action.</param>
    public static void OnCompleted<TOwner>(
        ref int stopped,
        TOwner owner,
        Action<TOwner> forward,
        Action<TOwner> dispose)
        where TOwner : class
    {
        if (Interlocked.Exchange(ref stopped, 1) != 0)
        {
            return;
        }

        try
        {
            forward(owner);
        }
        finally
        {
            dispose(owner);
        }
    }
}
