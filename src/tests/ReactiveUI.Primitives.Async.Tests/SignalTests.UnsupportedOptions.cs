// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the option-driven <see cref="Signal"/> factories reject missing and unknown publishing settings.</summary>
public partial class SignalTests
{
    /// <summary>A publishing option outside the defined values.</summary>
    private const PublishingOption UnknownPublishingOption = (PublishingOption)99;

    /// <summary>Missing options are rejected by every option-driven factory.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenSignalFactoryOptionsMissing_ThenThrowsArgumentOutOfRange()
    {
        await Assert.That(static () => Signal.Create<int>((SignalCreationOptions?)null)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => Signal.CreateBehavior(OptionsStartValue, (BehaviorSignalCreationOptions?)null))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => Signal.CreateReplayLatest<int>((ReplayLatestSignalCreationOptions?)null))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>An unknown publishing option is rejected by every option-driven factory.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenSignalFactoryPublishingOptionUnknown_ThenThrowsArgumentOutOfRange()
    {
        await Assert.That(static () => Signal.Create<int>(new SignalCreationOptions { PublishingOption = UnknownPublishingOption, IsStateless = false }))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => Signal.CreateBehavior(
                OptionsStartValue,
                new BehaviorSignalCreationOptions { PublishingOption = UnknownPublishingOption, IsStateless = false }))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => Signal.CreateReplayLatest<int>(
                new ReplayLatestSignalCreationOptions { PublishingOption = UnknownPublishingOption, IsStateless = false }))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
