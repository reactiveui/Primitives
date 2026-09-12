// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;

namespace ReactiveUI.Primitives.Reactive.Disposables;

/// <summary>Holds disposables and supports implicit conversion to a System.Reactive composite disposable.</summary>
/// <remarks>Conversions reuse a composite owned by the container. Composite registrations occupy one container slot and are not individually visible through Count, Contains, or Remove.</remarks>
[System.Diagnostics.DebuggerDisplay("ContainerDisposable: Count = {Count}, IsDisposed = {IsDisposed}")]
public sealed class ContainerDisposable : MultipleDisposable
{
    /// <summary>Serializes creation of the composite.</summary>
    private readonly Lock _gate = new();

    /// <summary>The composite handed to System.Reactive consumers, created on first conversion.</summary>
    private CompositeDisposable? _composite;

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class.</summary>
    public ContainerDisposable()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    public ContainerDisposable(IDisposable first, IDisposable second)
        : base(first, second)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class.</summary>
    /// <param name="first">The first disposable.</param>
    /// <param name="second">The second disposable.</param>
    /// <param name="third">The third disposable.</param>
    public ContainerDisposable(IDisposable first, IDisposable second, IDisposable third)
        : base(first, second, third)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ContainerDisposable"/> class from a group of disposables.</summary>
    /// <param name="disposables">Disposables that will be disposed together.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="disposables"/> is <see langword="null"/>.</exception>
    public ContainerDisposable(params IDisposable[] disposables)
        : base(disposables)
    {
    }

    /// <summary>Hands the container to a System.Reactive consumer as the composite it owns.</summary>
    /// <param name="container">The container to convert.</param>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="container"/> is <see langword="null"/>.</exception>
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
            Add(created);
            return created;
        }
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        // Repeated composite disposal has no effect.
        _composite?.Dispose();
    }
}
