// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests Reactive Extensions.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Sample integer value (2) used by multiple tests.</summary>
    private const int SampleValue2 = 2;

    /// <summary>Sample integer value (3) used by multiple tests.</summary>
    private const int SampleValue3 = 3;

    /// <summary>Sample integer value (4) used by multiple tests.</summary>
    private const int SampleValue4 = 4;

    /// <summary>Sample integer value (5) used by multiple tests.</summary>
    private const int SampleValue5 = 5;

    /// <summary>Sample integer value (6) used by multiple tests.</summary>
    private const int SampleValue6 = 6;

    /// <summary>Sample integer value (10) used by multiple tests.</summary>
    private const int SampleValue10 = 10;

    /// <summary>Sample integer value (20) used by multiple tests.</summary>
    private const int SampleValue20 = 20;

    /// <summary>Sample integer value (30) used by multiple tests.</summary>
    private const int SampleValue30 = 30;

    /// <summary>Sample integer value (42) used by multiple tests.</summary>
    private const int SampleValue42 = 42;

    /// <summary>Sample integer value (84) used by multiple tests.</summary>
    private const int SampleValue84 = 84;

    /// <summary>Sample integer value (7) used by multiple tests.</summary>
    private const int SampleValue7 = 7;

    /// <summary>Sample integer value (8) used by multiple tests.</summary>
    private const int SampleValue8 = 8;

    /// <summary>Sample integer value (9) used by multiple tests.</summary>
    private const int SampleValue9 = 9;

    /// <summary>Sample integer value (11) used by multiple tests.</summary>
    private const int SampleValue11 = 11;

    /// <summary>Sample integer value (13) used by multiple tests.</summary>
    private const int SampleValue13 = 13;

    /// <summary>Sample integer value (15) used by multiple tests.</summary>
    private const int SampleValue15 = 15;

    /// <summary>Sample integer value (99) used by multiple tests.</summary>
    private const int SampleValue99 = 99;

    /// <summary>Minimum-elapsed-time guard in milliseconds for timing assertions.</summary>
    private const int MinimumExpectedMilliseconds = 90;

    /// <summary>Throttle/debounce wait window in milliseconds.</summary>
    private const int ThrottleWaitMilliseconds = 150;

    /// <summary>Settle delay in milliseconds used by buffer/foreach tests.</summary>
    private const int SettleDelayMilliseconds = 200;

    /// <summary>Half of the scheduler window in virtual ticks.</summary>
    private const int SchedulerHalfWindowTicks = 50;

    /// <summary>Scheduler window length in virtual ticks.</summary>
    private const int SchedulerWindowTicks = 100;

    /// <summary>One tick past the scheduler window, used to advance virtual time.</summary>
    private const int SchedulerAdvancePastWindowTicks = 101;

    /// <summary>Short real-time delay in milliseconds.</summary>
    private const int ShortDelayMilliseconds = 500;

    /// <summary>Long real-time delay in milliseconds.</summary>
    private const int LongDelayMilliseconds = 1_000;

    /// <summary>Expected sequence [1, 2, 3] for collection equality assertions.</summary>
    private static readonly int[] ExpectedSequence123 = [1, 2, 3];

#if NET9_0_OR_GREATER
    /// <summary>Lock used to synchronize observer callbacks during concurrency tests.</summary>
    private readonly Lock _gate = new();
#else
    /// <summary>Lock used to synchronize observer callbacks during concurrency tests.</summary>
    private readonly object _gate = new();
#endif
}
