// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for detecting whether all values match a predicate.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AllPredicateWitness<T> : BooleanTerminalWitness<T>
{
    /// <summary>The predicate.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="AllPredicateWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate.</param>
    public AllPredicateWitness(IObserver<bool> observer, Func<T, bool> predicate)
        : base(observer)
    {
        _predicate = predicate;
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
            matches = _predicate(value);
        }
        catch (Exception error)
        {
            OnError(error);
            return;
        }

        if (matches)
        {
            return;
        }

        EmitCompleted(false);
    }

    /// <inheritdoc/>
    public override void OnCompleted() => EmitCompleted(true);
}
