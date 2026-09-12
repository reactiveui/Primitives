// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests default-scheduler retry overloads with synchronous retries.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Identifies the original error forwarded by a retry sequence.</summary>
    private const string RetryFailureMessage = "retry";

    /// <summary>Zero-delay retries notify the error callback only for the requested exception type.</summary>
    /// <param name="matchingException">Whether the first failure matches the error callback's type.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task OnErrorRetry_DefaultSchedulerZeroDelay_FiltersErrorCallback(bool matchingException)
    {
        var attempts = 0;
        List<InvalidOperationException> errors = [];
        List<int> values = [];
        Exception failure = matchingException
            ? new InvalidOperationException(RetryFailureMessage)
            : new ArgumentException(RetryFailureMessage);
        var source = Observable.Defer(() =>
        {
            attempts++;
            return attempts == 1 ? Observable.Throw<int>(failure) : Observable.Return(SampleValue42);
        });

        using var subscription = source.OnErrorRetry<int, InvalidOperationException>(errors.Add, TimeSpan.Zero)
            .Subscribe(values.Add);

        await Assert.That(attempts).IsEqualTo(SampleValue2);
        await Assert.That(values).IsCollectionEqualTo([SampleValue42]);
        await Assert.That(errors.Count).IsEqualTo(matchingException ? 1 : 0);
        if (!matchingException)
        {
            return;
        }

        await Assert.That(errors[0]).IsSameReferenceAs(failure);
    }

    /// <summary>The limited zero-delay overload forwards the final error after spending its retry budget.</summary>
    /// <param name="matchingException">Whether failures match the error callback's type.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task OnErrorRetry_DefaultSchedulerRetryLimit_ForwardsFinalError(bool matchingException)
    {
        var attempts = 0;
        var callbacks = 0;
        Exception? observed = null;
        Exception failure = matchingException
            ? new InvalidOperationException(RetryFailureMessage)
            : new ArgumentException(RetryFailureMessage);
        var source = Observable.Defer(() =>
        {
            attempts++;
            return Observable.Throw<int>(failure);
        });

        using var subscription = source.OnErrorRetry<int, InvalidOperationException>(
            _ => callbacks++,
            SampleValue2,
            TimeSpan.Zero).Subscribe(static _ => { }, error => observed = error);

        await Assert.That(attempts).IsEqualTo(SampleValue3);
        await Assert.That(callbacks).IsEqualTo(matchingException ? SampleValue3 : 0);
        await Assert.That(observed).IsSameReferenceAs(failure);
    }

    /// <summary>Fixed zero-delay retries use the configured budget before forwarding the original error.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RetryWithFixedDelay_ZeroDelay_ForwardsErrorAfterRetryLimit()
    {
        var attempts = 0;
        InvalidOperationException failure = new(RetryFailureMessage);
        Exception? observed = null;
        var source = Observable.Defer(() =>
        {
            attempts++;
            return Observable.Throw<int>(failure);
        });

        using var subscription = source.RetryWithFixedDelay(SampleValue2, TimeSpan.Zero)
            .Subscribe(static _ => { }, error => observed = error);

        await Assert.That(attempts).IsEqualTo(SampleValue3);
        await Assert.That(observed).IsSameReferenceAs(failure);
    }
}
