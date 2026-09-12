// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Reactive.Tests;

/// <summary>Tests scheduled loop cancellation after a scheduler has accepted its callback.</summary>
public class WhileObservableTests
{
    /// <summary>A callback already accepted by the scheduler does not invoke the action after disposal.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task While_DisposedBeforeAcceptedCallbackRuns_DoesNotInvokeAction()
    {
        AcceptedCallbackScheduler scheduler = new();
        var actions = 0;
        var values = 0;
        var completed = false;
        var subscription = ReactiveExtensions.While(static () => true, () => actions++, scheduler)
            .Subscribe(_ => values++, () => completed = true);

        subscription.Dispose();
        scheduler.RunAcceptedCallback();

        await Assert.That(actions).IsEqualTo(0);
        await Assert.That(values).IsEqualTo(0);
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Models best-effort cancellation after dispatch has accepted a callback for execution.</summary>
    private sealed class AcceptedCallbackScheduler : IScheduler
    {
        /// <summary>The accepted callback, which is no longer cancelable by its subscription.</summary>
        private Action? _accepted;

        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
        {
            _accepted = () => action(this, state).Dispose();
            return EmptyDisposable.Instance;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(
            TState state,
            TimeSpan dueTime,
            Func<IScheduler, TState, IDisposable> action) => Schedule(state, action);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(
            TState state,
            DateTimeOffset dueTime,
            Func<IScheduler, TState, IDisposable> action) => Schedule(state, action);

        /// <summary>Executes the accepted callback after the test has disposed its subscription.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RunAcceptedCallback() => _accepted!();
    }
}
