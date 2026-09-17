// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Reactive.Disposables;

/// <summary>Holds disposables and supports implicit conversion to a System.Reactive composite disposable.</summary>
/// <remarks>Conversions reuse a composite owned by the container. Composite registrations occupy one container slot and are not individually visible through Count, Contains, or Remove.</remarks>
[System.Diagnostics.DebuggerDisplay("ContainerDisposable: Count = {Count}, IsDisposed = {IsDisposed}")]
public sealed class ContainerDisposable : IsDisposed, ICollection<IDisposable>
{
    /// <summary>Serializes creation of the composite.</summary>
    private readonly Lock _gate = new();

    /// <summary>The held disposables.</summary>
    private DisposableSet _set;

    /// <summary>The composite handed to System.Reactive consumers, created on first conversion.</summary>
    private CompositeDisposable? _composite;

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class.</summary>
    public ContainerDisposable() => _set = new();

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    public ContainerDisposable(IDisposable first, IDisposable second) => _set = new(first, second);

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <param name="third">The third disposable.</param>
    public ContainerDisposable(IDisposable first, IDisposable second, IDisposable third) => _set = new(first, second, third);

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class from a group of disposables.</summary>
    /// <param name="disposables">Disposables that will be disposed together.</param>
    /// <exception cref="ArgumentNullException"><paramref name="disposables"/> is <see langword="null"/>.</exception>
    public ContainerDisposable(params IDisposable[] disposables) => _set = new(disposables);

    /// <summary>Gets a value indicating whether the object is disposed.</summary>
    public bool IsDisposed => _set.IsDisposed;

    /// <summary>Gets the number of held disposables, or zero after disposal.</summary>
    public int Count => _set.Count;

    /// <summary>Gets a value indicating whether the collection is read-only, which is always false.</summary>
    public bool IsReadOnly => false;

    /// <summary>Hands the container to a System.Reactive consumer as the composite it owns.</summary>
    /// <param name="container">The container to convert.</param>
    /// <exception cref="ArgumentNullException"><paramref name="container"/> is <see langword="null"/>.</exception>
    public static implicit operator CompositeDisposable(ContainerDisposable container)
    {
        ArgumentExceptionHelper.ThrowIfNull(container);

        return container.ToCompositeDisposable();
    }

    /// <summary>Gets the <see cref="CompositeDisposable"/> this container owns, creating it on first call.</summary>
    /// <returns>The composite whose contents are disposed along with this container.</returns>
    public CompositeDisposable ToCompositeDisposable()
    {
        lock (_gate)
        {
            // Disposed containers reject late additions; live containers replace composites removed by Clear or Remove.
            var existing = _composite;
            if (existing is not null && (!existing.IsDisposed || IsDisposed))
            {
                return existing;
            }

            var created = new CompositeDisposable();
            _composite = created;

            // Composite disposal follows container disposal.
            _set.Add(created);
            return created;
        }
    }

    /// <summary>Adds a disposable to the container, or disposes it immediately when the container is disposed.</summary>
    /// <param name="item">Disposable to add.</param>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(IDisposable item) => _set.Add(item);

    /// <summary>Removes and disposes the requested disposable from the container.</summary>
    /// <param name="item">Disposable to remove.</param>
    /// <returns><see langword="true"/> if the item was found and disposed; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="item"/> is null.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Remove(IDisposable? item) => _set.Remove(item);

    /// <summary>Removes and disposes every disposable currently held without disposing the container itself.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear() => _set.Clear();

    /// <summary>Determines whether the container currently holds the supplied disposable.</summary>
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

    /// <summary>Disposes every held disposable, including the owned composite; repeated calls have no further effect.</summary>
    public void Dispose()
    {
        _set.Dispose();

        // Repeated composite disposal has no effect.
        _composite?.Dispose();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
