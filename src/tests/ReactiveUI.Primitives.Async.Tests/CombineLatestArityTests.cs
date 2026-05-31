// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Generated tests for CombineLatest arities 2-8 covering catch blocks,
/// disposed guards in OnNextCombined/OnErrorResume, and OnNext_N early returns.
/// </summary>
public partial class CombineLatestArityTests
{
    /// <summary>Initial seed value emitted by source 2 in arity tests.</summary>
    private const int SeedValue2 = 2;

    /// <summary>Initial seed value emitted by source 3 in arity tests.</summary>
    private const int SeedValue3 = 3;

    /// <summary>Initial seed value emitted by source 4 in arity tests.</summary>
    private const int SeedValue4 = 4;

    /// <summary>Initial seed value emitted by source 5 in arity tests.</summary>
    private const int SeedValue5 = 5;

    /// <summary>Initial seed value emitted by source 6 in arity tests.</summary>
    private const int SeedValue6 = 6;

    /// <summary>Initial seed value emitted by source 7 in arity tests.</summary>
    private const int SeedValue7 = 7;

    /// <summary>Initial seed value emitted by source 8 in arity tests.</summary>
    private const int SeedValue8 = 8;

    /// <summary>Value emitted after the subscription is disposed; used to verify the post-dispose guard.</summary>
    private const int PostDisposeValue = 99;

    /// <summary>Sentinel value 42 used by disposal/error tests.</summary>
    private const int SentinelValue1 = 42;

    /// <summary>Sentinel value 43 used by disposal/error tests.</summary>
    private const int SentinelValue2 = 43;

    /// <summary>Sentinel value 44 used by disposal/error tests.</summary>
    private const int SentinelValue3 = 44;

    /// <summary>Sentinel value 45 used by disposal/error tests.</summary>
    private const int SentinelValue4 = 45;

    /// <summary>Sentinel value 46 used by disposal/error tests.</summary>
    private const int SentinelValue5 = 46;

    /// <summary>Sentinel value 47 used by disposal/error tests.</summary>
    private const int SentinelValue6 = 47;

    /// <summary>Place value 10 used to build unique per-source sums.</summary>
    private const int PlaceValue1 = 10;

    /// <summary>Place value 100 used to build unique per-source sums.</summary>
    private const int PlaceValue2 = 100;

    /// <summary>Place value 1,000 used to build unique per-source sums.</summary>
    private const int PlaceValue3 = 1_000;

    /// <summary>Place value 10,000 used to build unique per-source sums.</summary>
    private const int PlaceValue4 = 10_000;

    /// <summary>Place value 100,000 used to build unique per-source sums.</summary>
    private const int PlaceValue5 = 100_000;

    /// <summary>Place value 1,000,000 used to build unique per-source sums.</summary>
    private const int PlaceValue6 = 1_000_000;

    /// <summary>Place value 10,000,000 used to build unique per-source sums.</summary>
    private const int PlaceValue7 = 10_000_000;

    /// <summary>Place value 100,000,000 used by arity-9+ tests.</summary>
    private const int PlaceValue8 = 100_000_000;

    /// <summary>Place value 11 used by arity-10+ tests to keep sums unique without overflowing int.</summary>
    private const int PlaceValue9 = 11;

    /// <summary>Place value 13 used by arity-11+ tests.</summary>
    private const int PlaceValue10 = 13;

    /// <summary>Place value 17 used by arity-12+ tests.</summary>
    private const int PlaceValue11 = 17;

    /// <summary>Place value 19 used by arity-13+ tests.</summary>
    private const int PlaceValue12 = 19;

    /// <summary>Place value 23 used by arity-14+ tests.</summary>
    private const int PlaceValue13 = 23;

    /// <summary>Place value 29 used by arity-15+ tests.</summary>
    private const int PlaceValue14 = 29;

    /// <summary>Place value 31 used by arity-16 tests.</summary>
    private const int PlaceValue15 = 31;

    /// <summary>Re-emit value (2 × PlaceValue1) used to verify combined-value updates from source 2.</summary>
    private const int ReEmitValue2 = 20;

    /// <summary>Re-emit value (2 × PlaceValue2) used to verify combined-value updates from source 3.</summary>
    private const int ReEmitValue3 = 200;

    /// <summary>Re-emit value (2 × PlaceValue3) used to verify combined-value updates from source 4.</summary>
    private const int ReEmitValue4 = 2_000;

    /// <summary>Re-emit value (2 × PlaceValue4) used to verify combined-value updates from source 5.</summary>
    private const int ReEmitValue5 = 20_000;

    /// <summary>Re-emit value (2 × PlaceValue5) used to verify combined-value updates from source 6.</summary>
    private const int ReEmitValue6 = 200_000;

    /// <summary>Re-emit value (2 × PlaceValue6) used to verify combined-value updates from source 7.</summary>
    private const int ReEmitValue7 = 2_000_000;
}
