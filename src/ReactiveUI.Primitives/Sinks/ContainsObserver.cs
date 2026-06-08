// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for detecting whether a value is contained in a sequence.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class ContainsObserver<T> : BooleanTerminalObserver<T>
{
    /// <summary>The value to locate.</summary>
    private readonly T _value;

    /// <summary>The comparer used for equality checks.</summary>
    private readonly IEqualityComparer<T> _comparer;

    /// <summary>Initializes a new instance of the <see cref="ContainsObserver{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">The comparer used for equality checks.</param>
    public ContainsObserver(IObserver<bool> observer, T value, IEqualityComparer<T> comparer)
        : base(observer)
    {
        _value = value;
        _comparer = comparer;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        if (IsDone)
        {
            return;
        }

        bool matches;
        try
        {
            matches = _comparer.Equals(value, _value);
        }
        catch (Exception error)
        {
            OnError(error);
            return;
        }

        if (!matches)
        {
            return;
        }

        EmitCompleted(true);
    }

    /// <inheritdoc/>
    public override void OnCompleted() => EmitCompleted(false);
}
