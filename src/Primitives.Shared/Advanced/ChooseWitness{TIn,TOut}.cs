// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Applies the chooser to each value and forwards only the chosen ones.</summary>
/// <typeparam name="TIn">The source element type.</typeparam>
/// <typeparam name="TOut">The forwarded element type.</typeparam>
/// <param name="downstream">The downstream observer that receives the chosen values.</param>
/// <param name="chooser">Maps a source value to <c>(HasValue, Value)</c>; the value is skipped when <c>HasValue</c> is <see langword="false"/>.</param>
[System.Diagnostics.DebuggerDisplay("ChooseWitness: Subscription = {_subscription}")]
public sealed class ChooseWitness<TIn, TOut>(IObserver<TOut> downstream, Func<TIn, (bool HasValue, TOut Value)> chooser) : IObserver<TIn>, IDisposable
{
    /// <summary>The upstream subscription.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed atomically via WitnessLifetime.Dispose.")]
    private IDisposable? _subscription;

    /// <summary>Non-zero after termination or disposal.</summary>
    private int _stopped;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, ref _subscription);

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, ref _subscription, subscription);

    /// <inheritdoc/>
    public void OnNext(TIn value)
    {
        if (WitnessLifetime.IsStopped(ref _stopped))
        {
            return;
        }

        (bool HasValue, TOut Value) result;
        try
        {
            result = chooser(value);
        }
        catch (Exception ex) when (!FatalExceptionHelper.IsFatal(ex))
        {
            WitnessLifetime.Error(ref _stopped, ref _subscription, downstream, ex);
            return;
        }

        if (!result.HasValue)
        {
            return;
        }

        SinkDelivery.Next(downstream, result.Value, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => WitnessLifetime.Error(ref _stopped, ref _subscription, downstream, error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => WitnessLifetime.Complete(ref _stopped, ref _subscription, downstream);
}
