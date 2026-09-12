// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Reactive.Disposables;

namespace ReactiveUI.Primitives.Reactive;

/// <summary>Miscellaneous Primitives extensions.</summary>
public static partial class LinqExtensions
{
    /// <summary>Disposal-tracking operators for a disposable.</summary>
    /// <typeparam name="T">The disposable type.</typeparam>
    /// <param name="disposable">The disposable.</param>
    extension<T>(T disposable)
        where T : IDisposable
    {
        /// <summary>Disposes the IDisposable with the container.</summary>
        /// <param name="disposables">The container.</param>
        /// <returns>The original disposable.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="disposables"/> is <see langword="null"/>.</exception>
        /// <remarks>This exact container overload resolves DisposeWith calls when System.Reactive's composite overload is also in scope.</remarks>
        public T DisposeWith(ContainerDisposable disposables)
        {
            ArgumentExceptionHelper.ThrowIfNull(disposables);

            disposables.Add(disposable);
            return disposable;
        }
    }
}
