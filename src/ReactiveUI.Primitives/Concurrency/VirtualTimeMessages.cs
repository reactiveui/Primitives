// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
#if NET8_0_OR_GREATER
using System.Text;
#endif

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Builds the exception messages the virtual-time sequencers throw.</summary>
/// <remarks>
/// The parsed format sits on a non-generic type on purpose. A static field inside
/// <see cref="VirtualTimeState{TAbsolute, TRelative}"/> is a field of each closed generic, so the format would be
/// parsed once per type-argument pair; one shared instance serves every one of them. Frameworks without
/// <c>CompositeFormat</c> fall back to formatting the literal.
/// </remarks>
internal static class VirtualTimeMessages
{
    /// <summary>Format of the message thrown when the clock is advanced while the scheduler is already running.</summary>
    private const string SchedulerAlreadyRunningFormat =
        "{0} cannot be called when the scheduler is already running. Try using Sleep instead.";

#if NET8_0_OR_GREATER
    /// <summary>The parsed form of <see cref="SchedulerAlreadyRunningFormat"/>.</summary>
    private static readonly CompositeFormat SchedulerAlreadyRunningComposite =
        CompositeFormat.Parse(SchedulerAlreadyRunningFormat);
#endif

    /// <summary>Builds the message thrown when a clock-advancing method runs against an already-running scheduler.</summary>
    /// <param name="methodName">The name of the clock-advancing method that was called.</param>
    /// <returns>The formatted message.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string SchedulerAlreadyRunning(string methodName) =>
#if NET8_0_OR_GREATER
        string.Format(CultureInfo.CurrentCulture, SchedulerAlreadyRunningComposite, methodName);
#else
        string.Format(CultureInfo.CurrentCulture, SchedulerAlreadyRunningFormat, methodName);
#endif
}
