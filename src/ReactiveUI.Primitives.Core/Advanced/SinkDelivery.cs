// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Forwards a non-terminal value to a downstream observer and tears the sink down when that observer throws.</summary>
public static class SinkDelivery
{
    /// <summary>Forwards <paramref name="value"/> to <paramref name="observer"/>, disposing <paramref name="sink"/> when the observer throws.</summary>
    /// <typeparam name="TResult">The downstream result type.</typeparam>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The value to forward.</param>
    /// <param name="sink">The sink to dispose when <paramref name="observer"/> throws.</param>
    /// <remarks>The exception is rethrown once the sink has been disposed, so the upstream still sees it.</remarks>
    public static void Next<TResult>(IObserver<TResult> observer, TResult value, IDisposable sink)
    {
        try
        {
            observer.OnNext(value);
        }
        catch
        {
            sink.Dispose();
            throw;
        }
    }
}
