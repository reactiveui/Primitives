// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using Microsoft.Reactive.Testing;
using ReactiveLinqExtensions = ReactiveUI.Primitives.Reactive.LinqExtensions;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests the windowing and grouping operators compiled against System.Reactive.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>The duration of each window, in ticks.</summary>
    private const long SpanTicks = 10;

    /// <summary>The number of values in each count window.</summary>
    private const int WindowSize = 2;

    /// <summary>Slicing by time on a System.Reactive scheduler ends each window after the duration.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SliceEndsEachWindowAfterTheDurationOnASystemReactiveScheduler()
    {
        using Subject<string> source = new();
        TestScheduler scheduler = new();
        List<string> log = [];
        using var subscription = ReactiveLinqExtensions.Slice(source, TimeSpan.FromTicks(SpanTicks), scheduler).Subscribe(window =>
        {
            var name = $"w{log.Count(static entry => entry.StartsWith("open", StringComparison.Ordinal))}";
            log.Add($"open {name}");
            _ = window.Subscribe(value => log.Add($"{name}:{value}"), () => log.Add($"{name}:done"));
        });

        source.OnNext("a");
        scheduler.AdvanceBy(SpanTicks);
        source.OnNext("b");
        source.OnCompleted();

        await Assert.That(string.Join(' ', log)).IsEqualTo("open w0 w0:a w0:done open w1 w1:b w1:done");
    }

    /// <summary>Windowing by time and count on a System.Reactive scheduler ends a window at whichever limit comes first.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WindowEndsAWindowAtWhicheverLimitComesFirstOnASystemReactiveScheduler()
    {
        using Subject<string> source = new();
        TestScheduler scheduler = new();
        List<string> log = [];
        using var subscription = ReactiveLinqExtensions.Window(source, TimeSpan.FromTicks(SpanTicks), WindowSize, scheduler).Subscribe(window =>
        {
            var name = $"w{log.Count(static entry => entry.StartsWith("open", StringComparison.Ordinal))}";
            log.Add($"open {name}");
            _ = window.Subscribe(value => log.Add($"{name}:{value}"), () => log.Add($"{name}:done"));
        });

        source.OnNext("a");
        source.OnNext("b");
        scheduler.AdvanceBy(SpanTicks);
        source.OnCompleted();

        await Assert.That(string.Join(' ', log)).IsEqualTo("open w0 w0:a w0:b w0:done open w1 w1:done open w2 w2:done");
    }

    /// <summary>Grouping by key delivers a group per key over a System.Reactive subject.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GroupByDeliversAGroupPerKeyOverASystemReactiveSubject()
    {
        using Subject<string> source = new();
        List<string> log = [];
        using var subscription = ReactiveLinqExtensions.GroupBy(source, static value => value[..1]).Subscribe(group =>
        {
            log.Add($"open {group.Key}");
            _ = group.Subscribe(value => log.Add($"{group.Key}:{value}"), () => log.Add($"{group.Key}:done"));
        });

        source.OnNext("a1");
        source.OnNext("b1");
        source.OnNext("a2");
        source.OnCompleted();

        await Assert.That(string.Join(' ', log)).IsEqualTo("open a a:a1 open b b:b1 a:a2 a:done b:done");
    }

    /// <summary>Grouping until a duration ends a group when its duration signal emits.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GroupByUntilEndsAGroupWhenItsDurationSignalEmits()
    {
        using Subject<string> source = new();
        using Subject<int> duration = new();
        List<string> log = [];
        using var subscription = ReactiveLinqExtensions.GroupByUntil(source, static value => value[..1], _ => duration).Subscribe(group =>
        {
            log.Add($"open {group.Key}");
            _ = group.Subscribe(value => log.Add($"{group.Key}:{value}"), () => log.Add($"{group.Key}:done"));
        });

        source.OnNext("a1");
        duration.OnNext(0);
        source.OnNext("a2");

        await Assert.That(string.Join(' ', log)).IsEqualTo("open a a:a1 a:done open a a:a2");
    }
}
