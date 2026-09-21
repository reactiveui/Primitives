// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>System.Reactive-named windowing operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>System.Reactive-named windowing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the source values.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Splits the source into windows of a fixed number of values.</summary>
        /// <param name="count">The number of values in each window.</param>
        /// <returns>A sequence of window signals; the last window is shorter when the source ends mid-window.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(int count) => new SliceCountSignal<T>(source, count, count);

        /// <summary>Splits the source into windows of a fixed number of values, opening a new window every <paramref name="skip"/> values.</summary>
        /// <param name="count">The number of values in each window.</param>
        /// <param name="skip">The number of values between the starts of consecutive windows.</param>
        /// <returns>A sequence of window signals; windows overlap when <paramref name="skip"/> is smaller than <paramref name="count"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> or <paramref name="skip"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(int count, int skip) => new SliceCountSignal<T>(source, count, skip);

        /// <summary>Splits the source into consecutive windows of a fixed duration.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <returns>A sequence of window signals.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(TimeSpan timeSpan) =>
            new SliceTimeSignal<T>(source, timeSpan, timeSpan, Sequencer.Default);

        /// <summary>Splits the source into consecutive windows of a fixed duration on a scheduler.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <param name="scheduler">The scheduler that schedules the window timer.</param>
        /// <returns>A sequence of window signals.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(TimeSpan timeSpan, ISequencer scheduler) =>
            new SliceTimeSignal<T>(source, timeSpan, timeSpan, scheduler);

        /// <summary>Splits the source into windows of a fixed duration, opening a new window every <paramref name="timeShift"/>.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <param name="timeShift">The time between the starts of consecutive windows.</param>
        /// <returns>A sequence of window signals; windows overlap when <paramref name="timeShift"/> is shorter than <paramref name="timeSpan"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="timeShift"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(TimeSpan timeSpan, TimeSpan timeShift) =>
            new SliceTimeSignal<T>(source, timeSpan, timeShift, Sequencer.Default);

        /// <summary>Splits the source into windows of a fixed duration, opening a new window every <paramref name="timeShift"/>, on a scheduler.</summary>
        /// <param name="timeSpan">The duration of each window.</param>
        /// <param name="timeShift">The time between the starts of consecutive windows.</param>
        /// <param name="scheduler">The scheduler that schedules the window timer.</param>
        /// <returns>A sequence of window signals; windows overlap when <paramref name="timeShift"/> is shorter than <paramref name="timeSpan"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="timeShift"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(TimeSpan timeSpan, TimeSpan timeShift, ISequencer scheduler) =>
            new SliceTimeSignal<T>(source, timeSpan, timeShift, scheduler);

        /// <summary>Splits the source into windows that end after a duration or a number of values, whichever comes first.</summary>
        /// <param name="timeSpan">The maximum duration of each window.</param>
        /// <param name="count">The maximum number of values in each window.</param>
        /// <returns>A sequence of window signals; each new window restarts the duration.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="count"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(TimeSpan timeSpan, int count) =>
            new SliceTimeCountSignal<T>(source, timeSpan, count, Sequencer.Default);

        /// <summary>Splits the source into windows that end after a duration or a number of values, whichever comes first, on a scheduler.</summary>
        /// <param name="timeSpan">The maximum duration of each window.</param>
        /// <param name="count">The maximum number of values in each window.</param>
        /// <param name="scheduler">The scheduler that schedules the window timer.</param>
        /// <returns>A sequence of window signals; each new window restarts the duration.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> or <paramref name="count"/> is zero or negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window(TimeSpan timeSpan, int count, ISequencer scheduler) =>
            new SliceTimeCountSignal<T>(source, timeSpan, count, scheduler);

        /// <summary>Splits the source into windows, starting the next window each time a boundary signal emits.</summary>
        /// <typeparam name="TWindowBoundary">The value type of the boundary signal.</typeparam>
        /// <param name="windowBoundaries">The signal whose emissions start the next window.</param>
        /// <returns>A sequence of window signals; the sequence completes when the boundary signal completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="windowBoundaries"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window<TWindowBoundary>(IObservable<TWindowBoundary> windowBoundaries) =>
            new SliceBoundarySignal<T, TWindowBoundary>(source, windowBoundaries);

        /// <summary>Splits the source into consecutive windows, each ended by the signal the selector supplies for it.</summary>
        /// <typeparam name="TWindowClosing">The value type of the closing signals.</typeparam>
        /// <param name="windowClosingSelector">Supplies the signal that ends the window that has just opened.</param>
        /// <returns>A sequence of window signals; a window ends when its closing signal emits or completes.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="windowClosingSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window<TWindowClosing>(Func<IObservable<TWindowClosing>> windowClosingSelector) =>
            new SliceClosingSignal<T, TWindowClosing>(source, windowClosingSelector);

        /// <summary>Opens a window each time an opening signal emits and ends it with the signal the selector supplies for it.</summary>
        /// <typeparam name="TWindowOpening">The value type of the opening signal.</typeparam>
        /// <typeparam name="TWindowClosing">The value type of the closing signals.</typeparam>
        /// <param name="windowOpenings">The signal whose emissions open a window.</param>
        /// <param name="windowClosingSelector">Supplies the signal that ends the window an opening started.</param>
        /// <returns>A sequence of window signals; windows can overlap.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="windowOpenings"/> or <paramref name="windowClosingSelector"/> is <see langword="null"/>.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<IObservable<T>> Window<TWindowOpening, TWindowClosing>(
            IObservable<TWindowOpening> windowOpenings,
            Func<TWindowOpening, IObservable<TWindowClosing>> windowClosingSelector) =>
            new SliceOpeningSignal<T, TWindowOpening, TWindowClosing>(source, windowOpenings, windowClosingSelector);
    }
}
