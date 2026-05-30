// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.ComponentModel;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Fused replacement for
/// <c>Observable.FromEventPattern(...).Where(name match).Select(getter).StartWith(getter)</c>.
/// Subscribes directly to <see cref="INotifyPropertyChanged.PropertyChanged"/>, holds the
/// compiled getter once at construction (the original chain compiled it twice per
/// subscription), and emits the current value on subscribe followed by the getter
/// result for each matching property change — all through a single sink.
/// </summary>
/// <typeparam name="T">The owning type that raises <see cref="INotifyPropertyChanged.PropertyChanged"/>.</typeparam>
/// <typeparam name="TProperty">The property element type.</typeparam>
/// <param name="source">The owning instance.</param>
/// <param name="propertyName">The property name to filter by.</param>
/// <param name="getter">The compiled property getter (compiled once at construction).</param>
internal sealed class PropertyChangedObservable<T, TProperty>(
    T source,
    string propertyName,
    Func<T, TProperty> getter) : IObservable<TProperty>
    where T : INotifyPropertyChanged
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TProperty> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(propertyName);
        InvalidOperationExceptionHelper.ThrowIfNull(getter);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnNext(getter(source));

        var sink = new PropertyChangedSink(observer, source, propertyName, getter);
        source.PropertyChanged += sink.Handler;
        return sink;
    }

    /// <summary>
    /// Sink that holds the bound <see cref="PropertyChangedEventHandler"/> and detaches
    /// it on dispose. Filters by property name inline and pulls the value through the
    /// pre-compiled getter.
    /// </summary>
    private sealed class PropertyChangedSink : IDisposable
    {
        /// <summary>The downstream observer receiving filtered property values.</summary>
        private readonly IObserver<TProperty> _downstream;

        /// <summary>The owning instance whose event is being observed.</summary>
        private readonly T _source;

        /// <summary>The property name to filter on.</summary>
        private readonly string _propertyName;

        /// <summary>The pre-compiled property getter.</summary>
        private readonly Func<T, TProperty> _getter;

        /// <summary>Disposed flag (0 = active, 1 = disposed). Updated lock-free.</summary>
        private int _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PropertyChangedSink"/> class.
        /// </summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="source">The owning instance.</param>
        /// <param name="propertyName">The property name to filter on.</param>
        /// <param name="getter">The pre-compiled property getter.</param>
        public PropertyChangedSink(
            IObserver<TProperty> downstream,
            T source,
            string propertyName,
            Func<T, TProperty> getter)
        {
            _downstream = downstream;
            _source = source;
            _propertyName = propertyName;
            _getter = getter;
            Handler = OnPropertyChanged;
        }

        /// <summary>
        /// Gets the bound event handler attached to the source's
        /// <see cref="INotifyPropertyChanged.PropertyChanged"/> event.
        /// </summary>
        public PropertyChangedEventHandler Handler { get; }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _source.PropertyChanged -= Handler;
        }

        /// <summary>
        /// Bound <see cref="INotifyPropertyChanged.PropertyChanged"/> handler — filters
        /// by property name then forwards the pre-compiled getter result downstream.
        /// </summary>
        /// <param name="sender">Event sender (unused).</param>
        /// <param name="e">Event payload carrying the changed property name.</param>
        private void OnPropertyChanged(object? sender, PropertyChangedEventArgs e)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            if (e.PropertyName != _propertyName)
            {
                return;
            }

            TProperty value;
            try
            {
                value = _getter(_source);
            }
            catch (Exception ex)
            {
                _downstream.OnError(ex);
                Dispose();
                return;
            }

            _downstream.OnNext(value);
        }
    }
}
