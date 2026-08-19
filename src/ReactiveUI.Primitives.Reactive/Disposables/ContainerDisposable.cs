// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;

namespace ReactiveUI.Primitives.Reactive.Disposables;

/// <summary>
/// A <see cref="MultipleDisposable"/> that a System.Reactive consumer can use as a
/// <see cref="CompositeDisposable"/>, so an activation-scoped container flows into APIs written against
/// System.Reactive - <c>DisposeWith</c> above all - without the caller converting it by hand.
/// </summary>
/// <remarks>
/// <para>
/// The conversion is identity-stable: every conversion of the same container yields the same
/// <see cref="CompositeDisposable"/>, and the container owns that composite, so anything registered through it
/// is disposed when the container is. Registering after the container is disposed disposes the registration
/// immediately, matching <see cref="MultipleDisposable.Add"/>.
/// </para>
/// <para>
/// Registrations made through the composite are not visible to the container's own
/// <see cref="ICollection{T}"/> members: the composite occupies a single slot, so <c>Count</c> counts it once
/// and <c>Contains</c>/<c>Remove</c> do not see through it.
/// </para>
/// </remarks>
[System.Diagnostics.DebuggerDisplay("Count = {Count}, IsDisposed = {IsDisposed}")]
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
            // A disposed composite is still the right answer once the container itself is disposed - it is the
            // sink that disposes late arrivals. After Clear() or Remove() the container lives on, so a composite
            // it disposed has to be replaced rather than handed out again.
            var existing = _composite;
            if (existing is not null && (!existing.IsDisposed || IsDisposed))
            {
                return existing;
            }

            var created = new CompositeDisposable();
            _composite = created;

            // Registering the composite with the container is what ties the two lifetimes together. On an
            // already-disposed container this disposes the composite instead, which is what a caller adding to
            // a disposed container should get.
            Add(created);
            return created;
        }
    }

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        // The composite occupies a slot in the container, so the base disposed it just now - or Clear()/Remove()
        // did on the way out. Disposing it here is idempotent and states the ownership outright. Nothing in this
        // hierarchy has a finalizer and the class is sealed, so this only ever runs on the deterministic path.
        _composite?.Dispose();
    }
}
