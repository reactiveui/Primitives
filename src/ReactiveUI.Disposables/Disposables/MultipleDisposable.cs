// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Disposables;

/// <summary>A group of disposables that are disposed together.</summary>
[System.Diagnostics.DebuggerDisplay("MultipleDisposable: Count = {Count}, IsDisposed = {IsDisposed}")]
public sealed class MultipleDisposable : IsDisposed, ICollection<IDisposable>
{
    /// <summary>The held disposables.</summary>
    private DisposableSet _set;

    /// <summary>Initializes a new instance of the <see cref="MultipleDisposable"/> class.</summary>
    public MultipleDisposable() => _set = new();

    /// <summary>Initializes a new instance of the <see cref="MultipleDisposable"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    public MultipleDisposable(IDisposable first, IDisposable second) => _set = new(first, second);

    /// <summary>Initializes a new instance of the <see cref="MultipleDisposable"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <param name="third">The third disposable.</param>
    public MultipleDisposable(IDisposable first, IDisposable second, IDisposable third) => _set = new(first, second, third);

    /// <summary>Initializes a new instance of the <see cref="MultipleDisposable"/> class from a group of disposables.</summary>
    /// <param name="disposables">Disposables that will be disposed together.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposables"/> is <see langword="null"/>.</exception>
    public MultipleDisposable(params IDisposable[] disposables) => _set = new(disposables);

    /// <summary>Gets a value indicating whether the object is disposed.</summary>
    public bool IsDisposed => _set.IsDisposed;

    /// <summary>Gets the number of held disposables, or zero after disposal.</summary>
    public int Count => _set.Count;

    /// <summary>Gets a value indicating whether the collection is read-only, which is always false.</summary>
    public bool IsReadOnly => false;

    /// <summary>Creates a new group of disposable resources that are disposed together.</summary>
    /// <param name="disposables">Disposable resources to add to the group.</param>
    /// <returns>Group of disposable resources that are disposed together.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IDisposable Create(params IDisposable[] disposables) => new DisposableArray(disposables);

    /// <summary>Adds a disposable to the group, or disposes it immediately when the group is disposed.</summary>
    /// <param name="item">Disposable to add.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(IDisposable item) => _set.Add(item);

    /// <summary>Removes and disposes the requested disposable from the group.</summary>
    /// <param name="item">Disposable to remove.</param>
    /// <returns><see langword="true"/> if the item was found and disposed; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Remove(IDisposable? item) => _set.Remove(item);

    /// <summary>Removes and disposes every disposable currently held without disposing the group itself.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear() => _set.Clear();

    /// <summary>Determines whether the group currently holds the supplied disposable.</summary>
    /// <param name="item">Disposable to locate.</param>
    /// <returns><see langword="true"/> when the disposable is held; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(IDisposable item) => _set.Contains(item);

    /// <summary>Copies the disposables currently held into <paramref name="array"/> starting at <paramref name="arrayIndex"/>.</summary>
    /// <param name="array">Destination array.</param>
    /// <param name="arrayIndex">Zero-based index in <paramref name="array"/> at which copying begins.</param>
    /// <exception cref="ArgumentNullException"><paramref name="array"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="arrayIndex"/> is negative.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CopyTo(IDisposable[] array, int arrayIndex) => _set.CopyTo(array, arrayIndex);

    /// <summary>Returns an enumerator over a snapshot of the disposables currently held.</summary>
    /// <returns>An enumerator over the held disposables.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IEnumerator<IDisposable> GetEnumerator() => _set.Snapshot().GetEnumerator();

    /// <summary>Disposes every held disposable and marks the group disposed; repeated calls have no further effect.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _set.Dispose();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Array-backed disposable group returned by the static factory.</summary>
    private sealed class DisposableArray : IDisposable
    {
        /// <summary>Disposables to release, or <see langword="null"/> after disposal.</summary>
        private IDisposable[]? _disposables;

        /// <summary>Initializes a new instance of the <see cref="DisposableArray"/> class.</summary>
        /// <param name="disposables">Disposables owned by the group.</param>
        /// <exception cref="ArgumentNullException"><paramref name="disposables"/> is <see langword="null"/>.</exception>
        public DisposableArray(IDisposable[] disposables)
        {
            ArgumentExceptionHelper.ThrowIfNull(disposables);
            _disposables = disposables;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            var disposables = Interlocked.Exchange(ref _disposables, null);
            if (disposables is null)
            {
                return;
            }

            for (var i = 0; i < disposables.Length; i++)
            {
                disposables[i]?.Dispose();
            }
        }
    }
}
