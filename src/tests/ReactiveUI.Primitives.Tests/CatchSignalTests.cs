// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <c>Recover</c> over a sequence of sources walks them without holding a lock while user code runs.</summary>
public sealed class CatchSignalTests
{
    /// <summary>The value the first source emits.</summary>
    private const int First = 1;

    /// <summary>The value the second source emits.</summary>
    private const int Second = 2;

    /// <summary>An observer that marshals a value to another thread which fails the active source is not deadlocked, and the walk moves on to the next source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ObserverMarshallingAnErrorFromTheActiveSourceDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        IObserver<int>? firstObserver = null;
        List<int> values = [];
        var completions = 0;
        ScriptedObservable<int> first = new(observer =>
        {
            firstObserver = observer;
            observer.OnNext(First);
        });
        InvalidOperationException error = new("first-source");

        var worker = BackgroundThread.Start(() => _ = Signal.Recover(first, Signal.Emit(Second)).Subscribe(
            value =>
            {
                values.Add(value);
                if (value != First)
                {
                    return;
                }

                dispatcher.Invoke(() => firstObserver!.OnError(error));
            },
            static ex => throw ex,
            () => completions++));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        await Assert.That(values.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Disposing while the next source is being chosen stops the walk before that source is subscribed and releases the source sequence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposalWhileTheNextSourceIsChosenStopsTheWalk()
    {
        IObserver<int>? firstObserver = null;
        IDisposable? subscription = null;
        var secondSubscribed = false;
        ScriptedObservable<int> first = new(observer => firstObserver = observer);
        ScriptedObservable<int> second = new(_ => secondSubscribed = true);
        DisposingSources sources = new(first, second, () => subscription!.Dispose());
        RecordingWitness<int> downstream = new();

        subscription = sources.Recover().Subscribe(downstream);
        firstObserver!.OnError(new InvalidOperationException("first-source"));

        await Assert.That(secondSubscribed).IsFalse();
        await Assert.That(sources.IsReleased).IsTrue();
        await Assert.That(downstream.Errors.Count).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }

    /// <summary>A source sequence that runs an action before yielding its second source and records when it is released.</summary>
    /// <param name="first">The first source.</param>
    /// <param name="second">The second source.</param>
    /// <param name="beforeSecond">The action run while the second source is being chosen.</param>
    private sealed class DisposingSources(IObservable<int> first, IObservable<int> second, Action beforeSecond) : IEnumerable<IObservable<int>>
    {
        /// <summary>Gets a value indicating whether the enumerator has been disposed.</summary>
        public bool IsReleased { get; private set; }

        /// <inheritdoc/>
        public IEnumerator<IObservable<int>> GetEnumerator()
        {
            try
            {
                yield return first;
                beforeSecond();
                yield return second;
            }
            finally
            {
                IsReleased = true;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
