// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>ToPropertyObservable</c> backed by
/// <c>PropertyChangedObservable&lt;T, TProperty&gt;</c> — initial-value emission,
/// matching-name forwarding, unmatched-name filtering, getter-throws forwarding,
/// and dispose detaches the handler.</summary>
public class PropertyChangedObservableTests
{
    /// <summary>Initial property value.</summary>
    private const int InitialValue = 1;

    /// <summary>Updated property value.</summary>
    private const int UpdatedValue = 42;

    /// <summary>Verifies that subscribing emits the current property value immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribePropertyObservable_ThenEmitsCurrentValue()
    {
        ObservableOwner owner = new() { Value = InitialValue };
        List<int> results = [];

        using var sub = owner.ToPropertyObservable(x => x.Value)
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([InitialValue]);
    }

    /// <summary>Verifies that matching-name property changes are forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMatchingPropertyChanges_ThenForwardsValue()
    {
        ObservableOwner owner = new() { Value = InitialValue };
        List<int> results = [];

        using var sub = owner.ToPropertyObservable(x => x.Value)
            .Subscribe(results.Add);

        owner.Value = UpdatedValue;

        await Assert.That(results).IsCollectionEqualTo([InitialValue, UpdatedValue]);
    }

    /// <summary>Verifies that non-matching property changes are ignored.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUnrelatedPropertyChanges_ThenIgnored()
    {
        ObservableOwner owner = new() { Value = InitialValue };
        List<int> results = [];

        using var sub = owner.ToPropertyObservable(x => x.Value)
            .Subscribe(results.Add);

        owner.Other = "anything";

        await Assert.That(results).IsCollectionEqualTo([InitialValue]);
    }

    /// <summary>Verifies that disposing detaches the handler and stops forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscriptionDisposed_ThenNoFurtherEmissions()
    {
        ObservableOwner owner = new() { Value = InitialValue };
        List<int> results = [];

        var sub = owner.ToPropertyObservable(x => x.Value)
            .Subscribe(results.Add);
        sub.Dispose();

        owner.Value = UpdatedValue;

        await Assert.That(results).IsCollectionEqualTo([InitialValue]);
    }

    /// <summary>Verifies that an exception thrown by the getter on a property-changed callback
    /// is forwarded to <c>OnError</c>. The initial subscribe read succeeds; the second read (triggered
    /// by <c>Raise</c>) throws.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetterThrowsOnChange_ThenForwardsError()
    {
        LatchingThrowingOwner owner = new();
        Exception? caught = null;

        using var sub = owner.ToPropertyObservable(x => x.Latched)
            .Subscribe(static _ => { }, ex => caught = ex);

        owner.ArmAndRaise();

        await Assert.That(caught).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Exercises the <c>OnPropertyChanged</c> <c>_disposed</c> guard — fires a
    /// PropertyChanged event for the subscribed property after the subscription has been
    /// disposed but using an owner whose remove-handler is a no-op so the event delivery
    /// reaches the still-bound handler, which then sees <c>_disposed != 0</c> and returns.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPropertyEventFiresAfterDispose_ThenHandlerGuardSkipsForward()
    {
        RetainingObservableOwner owner = new();
        List<int> results = [];

        var sub = owner.ToPropertyObservable(x => x.Value).Subscribe(results.Add);

        sub.Dispose();

        // Even after Dispose, the retaining owner still references the handler — invoking the
        // event delivers to it, but the handler observes _disposed != 0 and returns early.
        owner.Raise();

        await Assert.That(results).IsCollectionEqualTo([0]);
    }

    /// <summary>INPC owner that retains every handler ever attached and exposes a manual
    /// <c>Raise</c> so a test can fire the PropertyChanged event after the subscription that
    /// added the handler has already been disposed.</summary>
    private sealed class RetainingObservableOwner : INotifyPropertyChanged
    {
        /// <summary>The retained handler list.</summary>
        private PropertyChangedEventHandler? _retained;

        /// <inheritdoc/>
        public event PropertyChangedEventHandler? PropertyChanged
        {
            add => _retained += value;
            remove
            {
                // Intentionally a no-op so disposed subscriptions stay reachable for Raise().
            }
        }

        /// <summary>Gets the observed property. This fixture never writes it, so every read yields zero —
        /// what is under test is the notification, not the value. It stays an instance auto-property
        /// because that is what the <c>ToPropertyObservable</c> expression tree resolves against.</summary>
        public int Value { get; }

        /// <summary>Invokes the retained handler with a <c>PropertyChanged</c> event for <see cref="Value"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Raise() => _retained?.Invoke(this, new(nameof(Value)));
    }

    /// <summary>Test owner that fires <see cref="INotifyPropertyChanged"/> on property writes.</summary>
    private sealed class ObservableOwner : INotifyPropertyChanged
    {
        /// <inheritdoc/>
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets or sets the value being observed by the test.</summary>
        public int Value
        {
            get;
            set
            {
                field = value;
                PropertyChanged?.Invoke(this, new(nameof(Value)));
            }
        }

        /// <summary>Gets or sets the unrelated property used to validate name-filtering.</summary>
        public string Other
        {
            get;
            set
            {
                field = value;
                PropertyChanged?.Invoke(this, new(nameof(Other)));
            }
        } = string.Empty;
    }

    /// <summary>Test owner whose getter succeeds on the first read but throws on subsequent reads.</summary>
    private sealed class LatchingThrowingOwner : INotifyPropertyChanged
    {
        /// <summary>Synthetic error message attached to getter failures.</summary>
        private const string GetterFailedMessage = "getter failed";

        /// <summary>Failure message used by the latched getter.</summary>
        private readonly string _message = GetterFailedMessage;

        /// <summary>Set after <see cref="ArmAndRaise"/> latches the getter into throwing mode.</summary>
        private bool _armed;

        /// <inheritdoc/>
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets a property that succeeds while disarmed and throws once armed.</summary>
        public int Latched => _armed ? throw new InvalidOperationException(_message) : 0;

        /// <summary>Arms the throwing behaviour and raises a change notification.</summary>
        public void ArmAndRaise()
        {
            _armed = true;
            PropertyChanged?.Invoke(this, new(nameof(Latched)));
        }
    }
}
