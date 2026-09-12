// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Witness"/> routing and safe-termination contracts.</summary>
public partial class WitnessTests
{
    /// <summary>Asserts each witness rejects the callback or observer it cannot work without.</summary>
    private static void AssertWitnessConstructorsRejectMissingCallbacks()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            CallbackWitness<int> invalid = new(null!, null, null);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            ForwardingWitness<int> invalid = new(null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            StatefulWitness<int, string> invalid = new(State, null!, null, null);
            GC.KeepAlive(invalid);
        });
    }

    /// <summary>Asserts a callback witness forwards each notification, and rethrows when no error callback was given.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertCallbackWitnessForwardsEachNotification()
    {
        List<int> callbackValues = [];
        List<Exception> callbackErrors = [];
        List<Result> callbackCompletions = [];
        CallbackWitness<int> callback = new(callbackValues.Add, callbackErrors.Add, callbackCompletions.Add);
        InvalidOperationException callbackError = new("callback");
        callback.OnNext(One);
        callback.OnError(callbackError);
        callback.OnCompleted();
        await Assert.That(callbackValues.SequenceEqual([One])).IsTrue();
        await Assert.That(callbackErrors[0]).IsSameReferenceAs(callbackError);
        await Assert.That(callbackCompletions[0].IsSuccess).IsTrue();
        InvalidOperationException callbackFallback = new("callback fallback");
        _ = Assert.Throws<InvalidOperationException>(() =>
            new CallbackWitness<int>(static _ => { }, null, null).OnError(callbackFallback));
        new CallbackWitness<int>(static _ => { }, null, null).OnCompleted();
    }

    /// <summary>Asserts a forwarding witness passes every notification through to the observer it wraps.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertForwardingWitnessForwardsEachNotification()
    {
        Recorder<int> forwarded = new();
        ForwardingWitness<int> forwarding = new(forwarded);
        InvalidOperationException forwardingError = new("forwarding");
        forwarding.OnNext(Two);
        forwarding.OnError(forwardingError);
        forwarding.OnCompleted();
        await Assert.That(forwarded.Values.SequenceEqual([Two])).IsTrue();
        await Assert.That(forwarded.Errors[0]).IsSameReferenceAs(forwardingError);
        await Assert.That(forwarded.Completed).IsEqualTo(1);
    }

    /// <summary>Asserts a stateful witness hands its state to every callback, and rethrows without an error callback.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertStatefulWitnessForwardsEachNotificationWithItsState()
    {
        List<string> statefulValues = [];
        List<string> statefulErrors = [];
        List<string> statefulCompletions = [];
        StatefulWitness<int, string> stateful = new(
            State,
            (value, state) => statefulValues.Add($"{state}:{value}"),
            (error, state) => statefulErrors.Add($"{state}:{error.Message}"),
            (result, state) => statefulCompletions.Add($"{state}:{result.IsSuccess}"));
        InvalidOperationException statefulError = new("stateful");
        stateful.OnNext(One);
        stateful.OnError(statefulError);
        stateful.OnCompleted();
        await Assert.That(statefulValues.SequenceEqual([$"{State}:{One}"])).IsTrue();
        await Assert.That(statefulErrors.SequenceEqual([$"{State}:{statefulError.Message}"])).IsTrue();
        await Assert.That(statefulCompletions.SequenceEqual([$"{State}:True"])).IsTrue();
        InvalidOperationException statefulFallback = new("stateful fallback");
        _ = Assert.Throws<InvalidOperationException>(() =>
            new StatefulWitness<int, string>(State, static (_, _) => { }, null, null).OnError(statefulFallback));
        new StatefulWitness<int, string>(State, static (_, _) => { }, null, null).OnCompleted();
    }

    /// <summary>Asserts a safe witness drops every notification that arrives after its first terminal one.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertSafeWitnessIgnoresNotificationsAfterTheTerminal()
    {
        List<int> safeValues = [];
        List<Exception> safeErrors = [];
        var safeCompleted = 0;
        var safe = Witness.Safe(Witness.Create<int>(safeValues.Add, safeErrors.Add, () => safeCompleted++));
        safe.OnNext(One);
        safe.OnCompleted();
        safe.OnNext(Two);
        safe.OnError(new InvalidOperationException("ignored"));
        safe.OnCompleted();
        await Assert.That(safeValues.SequenceEqual([One])).IsTrue();
        await Assert.That(safeErrors.Count).IsEqualTo(0);
        await Assert.That(safeCompleted).IsEqualTo(1);
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => Values.Add(value);
    }

    /// <summary>Observable with a disposable subscription tracker and captured observer.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class RecordingDisposableObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Gets the number of times the source subscription was disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return new ActionDisposable(() => DisposeCount++);
        }
    }
}
