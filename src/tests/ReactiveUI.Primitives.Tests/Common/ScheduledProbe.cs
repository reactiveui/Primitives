// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>A scheduled item probe that delegates invocation to a supplied factory.</summary>
internal sealed class ScheduledProbe : ScheduledItem<int>
{
    /// <summary>The factory invoked when the item runs.</summary>
    private readonly Func<IDisposable> _invoke;

    /// <summary>Initializes a new instance of the <see cref="ScheduledProbe"/> class.</summary>
    /// <param name="dueTime">The due time for the scheduled item.</param>
    /// <param name="invoke">The factory invoked when the item runs.</param>
    public ScheduledProbe(int dueTime, Func<IDisposable> invoke)
        : base(dueTime, Comparer<int>.Default) => _invoke = invoke;

    /// <summary>Invokes the supplied factory.</summary>
    /// <returns>The disposable returned by the factory.</returns>
    protected override IDisposable InvokeCore() => _invoke();
}
