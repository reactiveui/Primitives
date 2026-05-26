// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#pragma warning disable SA1201 // Debugger display partial members are grouped by namespace.
#pragma warning disable SA1402 // Debugger display partial members are intentionally grouped in one support file.
#pragma warning disable SA1403 // Debugger display partial members span the public namespaces that need the shared pattern.
#pragma warning disable SA1601 // Primary type declarations carry the public documentation.

namespace ReactiveUI.Primitives
{
    public sealed partial class ConnectableSignal<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public readonly partial struct RxVoid
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }
}

namespace ReactiveUI.Primitives.Concurrency
{
    public sealed partial class CurrentThreadSequencer
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

#if WINDOWS
    public partial class DispatcherSequencer
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }
#endif

    public sealed partial class ImmediateSequencer
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public abstract partial class ScheduledItem<TAbsolute>
        where TAbsolute : IComparable<TAbsolute>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class ScheduledItem<TAbsolute, TValue>
        where TAbsolute : IComparable<TAbsolute>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class SequencerQueue<TAbsolute>
        where TAbsolute : IComparable<TAbsolute>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class TaskPoolSequencer
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class TestClock
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class ThreadPoolSequencer
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class VirtualClock
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public abstract partial class VirtualTimeSequencer<TAbsolute, TRelative>
        where TAbsolute : IComparable<TAbsolute>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public abstract partial class VirtualTimeSequencerBase<TAbsolute, TRelative>
        where TAbsolute : IComparable<TAbsolute>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }
}

namespace ReactiveUI.Primitives.Core
{
    public readonly partial struct Moment<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public abstract partial class Spark<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public readonly partial struct TimeInterval<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }
}

namespace ReactiveUI.Primitives.Disposables
{
    public sealed partial class AssignmentSlot
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class BooleanDisposable
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class CancellationDisposable
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class MultipleDisposable
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class Pocket
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class SingleDisposable
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class SingleReplaceableDisposable
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class Slot
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }
}

namespace ReactiveUI.Primitives.Signals
{
    public partial class AsyncSignal<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class CommandSignal<TResult>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public sealed partial class ReadOnlyState<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class ReplaySignal<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class Signal<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }

    public partial class StateSignal<T>
    {
        /// <summary>
        /// Gets the debugger display text.
        /// </summary>
        [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
        private string DebuggerDisplay => ToString() ?? string.Empty;
    }
}
