// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Blazor.Concurrency;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Tests;

/// <summary>Tests for <see cref="BlazorRendererSequencer"/> driven through a fake renderer delegate.</summary>
public sealed class BlazorRendererSequencerTests
{
    /// <summary>Expected values produced by an immediate burst, used to verify FIFO order.</summary>
    private static readonly int[] ExpectedBurst = [1, 2, 3];

    /// <summary>Verifies the constructor rejects a null renderer delegate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDelegate() =>
        await Assert.That(() => new BlazorRendererSequencer(null!)).Throws<ArgumentNullException>();

    /// <summary>Verifies immediate work is marshalled through the renderer delegate and executed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleMarshalsThroughRenderer()
    {
        var renderer = new FakeRenderer();
        var sequencer = new BlazorRendererSequencer(renderer.InvokeAsync);
        var executed = false;

        sequencer.Schedule(new DelegateWorkItem(() => executed = true));

        await Assert.That(executed).IsTrue();
        await Assert.That(renderer.InvokeCount).IsGreaterThan(0);
    }

    /// <summary>Verifies a burst of immediate work items all execute in FIFO order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateBurstExecutesInOrder()
    {
        var renderer = new FakeRenderer();
        var sequencer = new BlazorRendererSequencer(renderer.InvokeAsync);
        var values = new List<int>();

        foreach (var value in ExpectedBurst)
        {
            var captured = value;
            sequencer.Schedule(new DelegateWorkItem(() => values.Add(captured)));
        }

        await Assert.That(values).IsEquivalentTo(ExpectedBurst, EqualityComparer<int>.Default);
    }

    /// <summary>Work item that invokes a delegate when executed.</summary>
    private sealed class DelegateWorkItem : IWorkItem
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action;

        /// <summary>Initializes a new instance of the <see cref="DelegateWorkItem"/> class.</summary>
        /// <param name="action">The action to run on execution.</param>
        public DelegateWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        public void Execute() => _action();
    }

    /// <summary>Fake renderer that runs marshalled work synchronously and records how often it was invoked.</summary>
    private sealed class FakeRenderer
    {
        /// <summary>Gets the number of times <see cref="InvokeAsync(Action)"/> was called.</summary>
        public int InvokeCount { get; private set; }

        /// <summary>Runs the supplied work synchronously, mimicking <c>ComponentBase.InvokeAsync</c>.</summary>
        /// <param name="action">The work to run.</param>
        /// <returns>A completed task.</returns>
        public Task InvokeAsync(Action action)
        {
            InvokeCount++;
            action();
            return Task.CompletedTask;
        }
    }
}
