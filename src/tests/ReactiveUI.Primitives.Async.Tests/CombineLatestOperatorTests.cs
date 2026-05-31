// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Comprehensive tests for the CombineLatest operator covering all arities (2-8),
/// error propagation, completion, error resume, disposal, and edge cases.
/// </summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Source count for arity-4 CombineLatest tests.</summary>
    private const int FourSources = 4;

    /// <summary>Source count for arity-5 CombineLatest tests.</summary>
    private const int FiveSources = 5;

    /// <summary>Source count for arity-6 CombineLatest tests.</summary>
    private const int SixSources = 6;

    /// <summary>Source count for arity-7 CombineLatest tests.</summary>
    private const int SevenSources = 7;

    /// <summary>Source count for arity-8 CombineLatest tests.</summary>
    private const int EightSources = 8;

    /// <summary>Seed value emitted by the first source.</summary>
    private const int Source1Value = 2;

    /// <summary>Seed value emitted by the second source.</summary>
    private const int Source2Value = 3;

    /// <summary>Seed value emitted by the third source.</summary>
    private const int Source3Value = 4;

    /// <summary>Seed value emitted by the fourth source.</summary>
    private const int Source4Value = 5;

    /// <summary>Seed value emitted by the fifth source.</summary>
    private const int Source5Value = 6;

    /// <summary>Seed value emitted by the sixth source.</summary>
    private const int Source6Value = 7;

    /// <summary>Seed value emitted by the seventh source.</summary>
    private const int Source7Value = 8;

    /// <summary>Second-emission value for source 1 in the per-source progression ladder.</summary>
    private const int Step1 = 10;

    /// <summary>Second-emission value for source 2 in the per-source progression ladder.</summary>
    private const int Step2 = 20;

    /// <summary>Second-emission value for source 3 in the per-source progression ladder.</summary>
    private const int Step3 = 30;

    /// <summary>Second-emission value for source 4 in the per-source progression ladder.</summary>
    private const int Step4 = 40;

    /// <summary>Second-emission value for source 5 in the per-source progression ladder.</summary>
    private const int Step5 = 50;

    /// <summary>Second-emission value for source 6 in the per-source progression ladder.</summary>
    private const int Step6 = 60;

    /// <summary>Second-emission value for source 7 in the per-source progression ladder.</summary>
    private const int Step7 = 70;

    /// <summary>Second-emission value for source 8 in the per-source progression ladder.</summary>
    private const int Step8 = 80;

    /// <summary>Large step value (100) used in scale tests.</summary>
    private const int LargeStep1 = 100;

    /// <summary>Large step value (200) used in scale tests.</summary>
    private const int LargeStep2 = 200;

    /// <summary>Large step value (1000) used in scale tests.</summary>
    private const int LargeStep3 = 1_000;

    /// <summary>Sentinel value (99) used for late or post-disposal emissions.</summary>
    private const int SentinelValue = 99;

    /// <summary>Timeout in seconds for awaiting test completion signals.</summary>
    private const int WaitTimeoutSeconds = 5;

    /// <summary>Index of source 2 within the sources array.</summary>
    private const int Source2Index = 2;

    /// <summary>Index of source 3 within the sources array.</summary>
    private const int Source3Index = 3;

    /// <summary>Index of source 4 within the sources array.</summary>
    private const int Source4Index = 4;

    /// <summary>Index of source 5 within the sources array.</summary>
    private const int Source5Index = 5;

    /// <summary>Index of source 6 within the sources array.</summary>
    private const int Source6Index = 6;

    /// <summary>Index of source 7 within the sources array.</summary>
    private const int Source7Index = 7;
}
