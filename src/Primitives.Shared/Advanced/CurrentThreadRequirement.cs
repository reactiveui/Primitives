// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Shared current-thread requirement checks for advanced signals.</summary>
internal static class CurrentThreadRequirement
{
    /// <summary>Determines whether a source requires current-thread subscription.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public static bool IsRequired<T>(IObservable<T> source)
    {
        if (source is not IRequireCurrentThread<T> currentThread)
        {
            return false;
        }

        return currentThread.IsRequiredSubscribeOnCurrentThread();
    }
}
