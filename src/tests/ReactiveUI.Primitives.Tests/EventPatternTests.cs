// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Specialized;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="EventPattern{TEventArgs}"/> equality and formatting contracts.</summary>
public class EventPatternTests
{
    /// <summary>Covers event-pattern equality, hashing, formatting, and argument validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EventPatternEqualityAndFormattingCoverContracts()
    {
        object sender = new();
        var args = EventArgs.Empty;
        EventPattern<EventArgs> pattern = new(sender, args);
        EventPattern<EventArgs> same = new(sender, args);
        EventPattern<EventArgs> other = new(new(), args);
        await Assert.That(pattern == same).IsTrue();
        await Assert.That(pattern != other).IsTrue();
        await Assert.That(pattern.Equals((object)same)).IsTrue();
        await Assert.That(pattern.Equals("not an event")).IsFalse();
        await Assert.That(pattern.GetHashCode()).IsNotEqualTo(0);
        await Assert.That(pattern.ToString().Contains(nameof(EventArgs), StringComparison.Ordinal)).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            EventPattern<EventArgs> invalid = new(sender, null!);
            GC.KeepAlive(invalid);
        });
    }

    /// <summary>Verifies generic event factory overloads for supported and unsupported handler shapes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GenericFromEventPatternCoversPropertyChangedGenericAndUnsupportedHandlers()
    {
        const int EventValue = 7;
        GenericEventSource source = new();
        List<int> values = [];
        var genericSubscription = Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(
                handler => source.Changed += handler,
                handler => source.Changed -= handler)
            .Subscribe(pattern => values.Add(pattern.EventArgs.Value));
        source.Raise(EventValue);
        genericSubscription.Dispose();
        source.Raise(EventValue + 1);
        await Assert.That(values.SequenceEqual([EventValue])).IsTrue();
        PropertyChangedEventSource propertySource = new();
        List<string?> propertyNames = [];
        var propertySubscription = Signal.FromEventPattern<PropertyChangedEventHandler, PropertyChangedEventArgs>(
                handler => propertySource.PropertyChanged += handler,
                handler => propertySource.PropertyChanged -= handler)
            .Subscribe(pattern => propertyNames.Add(pattern.EventArgs.PropertyName));
        propertySource.Raise(nameof(PropertyChangedEventSource.Value));
        propertySubscription.Dispose();
        propertySource.Raise("ignored");
        await Assert.That(propertyNames.SequenceEqual([nameof(PropertyChangedEventSource.Value)])).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() =>
            Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(null!, _ => { }));
        _ = Assert.Throws<ArgumentNullException>(() =>
            Signal.FromEventPattern<EventHandler<TestEventArgs>, TestEventArgs>(_ => { }, null!));
        _ = Assert.Throws<NotSupportedException>(() =>
            Signal.FromEventPattern<Action, EventArgs>(_ => { }, _ => { }).Subscribe(_ => { }));
    }

    /// <summary>Verifies generic event factories support WPF-style non-generic event handler shapes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GenericFromEventPatternSupportsCompatibleNonGenericHandlerShape()
    {
        AssemblyLoadEventSource source = new();
        var assembly = typeof(EventPatternTests).Assembly;
        List<EventPattern<AssemblyLoadEventArgs>> events = [];
        using (Signal.FromEventPattern<AssemblyLoadEventHandler, AssemblyLoadEventArgs>(
                   handler => source.AssemblyLoaded += handler,
                   handler => source.AssemblyLoaded -= handler).Subscribe(events.Add))
        {
            source.Raise(assembly);
        }

        source.Raise(typeof(string).Assembly);

        await Assert.That(events).Count().IsEqualTo(1);
        await Assert.That(events[0].Sender!).IsSameReferenceAs(source);
        await Assert.That(events[0].EventArgs.LoadedAssembly).IsSameReferenceAs(assembly);
        await Assert.That(source.SubscriberCount).IsEqualTo(0);
    }

    /// <summary>Verifies generic event factories support collection and list event handlers used by binding sources.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GenericFromEventPatternSupportsCollectionAndListHandlers()
    {
        CollectionChangedEventSource collectionSource = new();
        List<NotifyCollectionChangedAction> collectionActions = [];
        var collectionSubscription =
            Signal.FromEventPattern<NotifyCollectionChangedEventHandler, NotifyCollectionChangedEventArgs>(
                    handler => collectionSource.CollectionChanged += handler,
                    handler => collectionSource.CollectionChanged -= handler)
                .Subscribe(pattern => collectionActions.Add(pattern.EventArgs.Action));

        collectionSource.RaiseAdd();
        collectionSubscription.Dispose();
        collectionSource.RaiseAdd();

        await Assert.That(collectionActions.SequenceEqual([NotifyCollectionChangedAction.Add])).IsTrue();

        ListChangedEventSource listSource = new();
        List<ListChangedType> listChangeTypes = [];
        var listSubscription = Signal.FromEventPattern<ListChangedEventHandler, ListChangedEventArgs>(
                handler => listSource.ListChanged += handler,
                handler => listSource.ListChanged -= handler)
            .Subscribe(pattern => listChangeTypes.Add(pattern.EventArgs.ListChangedType));

        listSource.RaiseAdd();
        listSubscription.Dispose();
        listSource.RaiseAdd();

        await Assert.That(listChangeTypes.SequenceEqual([ListChangedType.ItemAdded])).IsTrue();
    }

    /// <summary>Verifies event-pattern bridges preserve sender/arguments and detach handlers on disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EventPatternBridgePreservesSenderArgumentsAndDisposesHandler()
    {
        const int ExpectedEventCount = 2;
        FakeButton button = new();
        List<EventPattern<FakeClickEventArgs>> events = [];
        using (Signal.FromEventPattern<FakeClickEventArgs>(
                   handler => button.Clicked += handler,
                   handler => button.Clicked -= handler).Subscribe(events.Add))
        {
            button.Raise("open");
            button.Raise("save");
        }

        button.Raise("ignored");
        await Assert.That(events.Count).IsEqualTo(ExpectedEventCount);
        await Assert.That(events[0].Sender!).IsSameReferenceAs(button);
        await Assert.That(events[0].EventArgs.Command).IsEqualTo("open");
        await Assert.That(events[1].EventArgs.Command).IsEqualTo("save");
        await Assert.That(button.SubscriberCount).IsEqualTo(0);
        var expectedDescription = $"{typeof(FakeButton).FullName}: {typeof(FakeClickEventArgs).FullName}";
        await Assert.That(events[0].ToString()).IsEqualTo(expectedDescription);
    }

    /// <summary>Event arguments carrying a deterministic integer value.</summary>
    /// <param name="value">The value supplied by the event.</param>
    private sealed class TestEventArgs(int value) : EventArgs
    {
        /// <summary>Gets the event value.</summary>
        public int Value { get; } = value;
    }

    /// <summary>Source used to exercise generic <see cref="EventHandler{TEventArgs}"/> event conversion.</summary>
    private sealed class GenericEventSource
    {
        /// <summary>Raised by the test source.</summary>
        public event EventHandler<TestEventArgs>? Changed;

        /// <summary>Raises <see cref="Changed"/> with the supplied value.</summary>
        /// <param name="value">The value supplied to the event arguments.</param>
        public void Raise(int value) => Changed?.Invoke(this, new(value));
    }

    /// <summary>Source used to exercise a WPF-style non-generic event handler shape.</summary>
    private sealed class AssemblyLoadEventSource
    {
        /// <summary>Raised by the test source.</summary>
        [SuppressMessage(
            "Roslynator",
            "RCS1159:Use EventHandler<T>",
            Justification = "This test deliberately covers WPF-style non-generic event handler delegates.")]
        [SuppressMessage(
            "Major Code Smell",
            "S3908:Refactor this delegate to use 'System.EventHandler<TEventArgs>'.",
            Justification = "This test deliberately covers WPF-style non-generic event handler delegates.")]
        [SuppressMessage(
            "Major Code Smell",
            "S3906:Event Handlers should have the correct signature",
            Justification = "This test deliberately covers WPF-style non-generic event handler delegates.")]
        public event AssemblyLoadEventHandler? AssemblyLoaded;

        /// <summary>Gets the current subscriber count.</summary>
        public int SubscriberCount => AssemblyLoaded?.GetInvocationList().Length ?? 0;

        /// <summary>Raises <see cref="AssemblyLoaded"/> with the supplied assembly.</summary>
        /// <param name="assembly">The assembly supplied to the event arguments.</param>
        public void Raise(System.Reflection.Assembly assembly) => AssemblyLoaded?.Invoke(this, new(assembly));
    }

    /// <summary>Source used to exercise <see cref="PropertyChangedEventHandler"/> event conversion.</summary>
    private sealed class PropertyChangedEventSource
    {
        /// <summary>Raised by the test source.</summary>
        [SuppressMessage(
            "Roslynator",
            "RCS1159:Use EventHandler<T>",
            Justification =
                "This test deliberately covers the PropertyChangedEventHandler branch of the factory overload.")]
        [SuppressMessage(
            "Major Code Smell",
            "S3908:Refactor this delegate to use 'System.EventHandler<TEventArgs>'.",
            Justification =
                "This test deliberately covers the PropertyChangedEventHandler branch of the factory overload.")]
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets a placeholder property name used by the event test.</summary>
        public static int Value => 0;

        /// <summary>Raises <see cref="PropertyChanged"/> with the supplied property name.</summary>
        /// <param name="propertyName">The property name supplied to the event arguments.</param>
        public void Raise(string propertyName) => PropertyChanged?.Invoke(this, new(propertyName));
    }

    /// <summary>Source used to exercise <see cref="NotifyCollectionChangedEventHandler"/> event conversion.</summary>
    private sealed class CollectionChangedEventSource
    {
        /// <summary>Raised by the test source.</summary>
        [SuppressMessage(
            "Roslynator",
            "RCS1159:Use EventHandler<T>",
            Justification =
                "This test deliberately covers the NotifyCollectionChangedEventHandler branch of the factory overload.")]
        [SuppressMessage(
            "Major Code Smell",
            "S3908:Refactor this delegate to use 'System.EventHandler<TEventArgs>'.",
            Justification =
                "This test deliberately covers the NotifyCollectionChangedEventHandler branch of the factory overload.")]
        public event NotifyCollectionChangedEventHandler? CollectionChanged;

        /// <summary>Raises <see cref="CollectionChanged"/> with an add action.</summary>
        public void RaiseAdd() =>
            CollectionChanged?.Invoke(this, new(NotifyCollectionChangedAction.Add, "value"));
    }

    /// <summary>Source used to exercise <see cref="ListChangedEventHandler"/> event conversion.</summary>
    private sealed class ListChangedEventSource
    {
        /// <summary>Raised by the test source.</summary>
        [SuppressMessage(
            "Roslynator",
            "RCS1159:Use EventHandler<T>",
            Justification =
                "This test deliberately covers the ListChangedEventHandler branch of the factory overload.")]
        [SuppressMessage(
            "Major Code Smell",
            "S3908:Refactor this delegate to use 'System.EventHandler<TEventArgs>'.",
            Justification =
                "This test deliberately covers the ListChangedEventHandler branch of the factory overload.")]
        public event ListChangedEventHandler? ListChanged;

        /// <summary>Raises <see cref="ListChanged"/> with an item-added action.</summary>
        public void RaiseAdd() => ListChanged?.Invoke(this, new(ListChangedType.ItemAdded, 0));
    }

    /// <summary>Event arguments for fake click events.</summary>
    private sealed class FakeClickEventArgs : EventArgs
    {
        /// <summary>Initializes a new instance of the <see cref="FakeClickEventArgs"/> class.</summary>
        /// <param name="command">The command associated with the click.</param>
        public FakeClickEventArgs(string command) => Command = command;

        /// <summary>Gets the command associated with the click.</summary>
        public string Command { get; }
    }

    /// <summary>Fake event source used by event-pattern bridge scenarios.</summary>
    private sealed class FakeButton
    {
        /// <summary>Raised when the fake button is clicked.</summary>
        public event EventHandler<FakeClickEventArgs>? Clicked;

        /// <summary>Gets the current subscriber count.</summary>
        public int SubscriberCount => Clicked?.GetInvocationList().Length ?? 0;

        /// <summary>Raises the fake click event.</summary>
        /// <param name="command">The click command.</param>
        public void Raise(string command) => Clicked?.Invoke(this, new(command));
    }
}
