// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Spark"/> value, error, completion, and equality contracts.</summary>
public class SparkTests
{
    /// <summary>Event text recorded by the delegate-based completion callback.</summary>
    private const string DelegateCompletedText = "delegate-completed";

    /// <summary>Forty-two as a named value.</summary>
    private const int FortyTwo = 42;

    /// <summary>Forty-three as a named value.</summary>
    private const int FortyThree = 43;

    /// <summary>Shared completed result text.</summary>
    private const string CompletedText = "completed";

    /// <summary>Shared completed function result text.</summary>
    private const string FunctionCompletedText = "fn-completed";

    /// <summary>Expected observable values.</summary>
    private static readonly int[] ExpectedObservableValues = [FortyTwo];

    /// <summary>Verifies completed sparks compare equal by value for each value type.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletedSparksAreEqualPerValueType()
    {
        var first = Spark.CreateOnCompleted<int>();
        var second = Spark.CreateOnCompleted<int>();
        await Assert.That(first == second).IsTrue();
        await Assert.That(second).IsEqualTo(first);
    }

    /// <summary>Exercises on-next spark value, equality, accept overloads, and observable conversion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SparkOnNextCoversValueEqualityAndAcceptOverloads()
    {
        var next = Spark.CreateOnNext(FortyTwo);
        var sameNext = Spark.CreateOnNext(FortyTwo);
        var differentNext = Spark.CreateOnNext(FortyThree);
        var completed = Spark.CreateOnCompleted<int>();
        RecordingResultWitness<int> observer = new();
        List<int> observableValues = [];
        var observableCompleted = 0;
        await Assert.That(next == sameNext).IsTrue();
        await Assert.That(next != differentNext).IsTrue();
        await Assert.That(next.Equals(completed)).IsFalse();
        await Assert.That(next.HasValue).IsTrue();
        await Assert.That(next.Value).IsEqualTo(FortyTwo);
        await Assert.That(next.Kind).IsEqualTo(SparkKind.OnNext);
        await Assert.That(next.ToString().Contains(FortyTwo.ToString(), StringComparison.Ordinal)).IsTrue();
        await Assert.That(sameNext.GetHashCode()).IsEqualTo(next.GetHashCode());
        next.Accept((IObserver<int>)observer);
        await Assert.That(next.Accept<string>(observer)).IsEqualTo("next:42");
        next.Accept(
            value => observer.Events.Add($"delegate-next:{value}"),
            ex => observer.Events.Add(ex.Message),
            () => observer.Events.Add(DelegateCompletedText));
        await Assert.That(next.Accept(static value => $"fn-next:{value}", static ex => ex.Message, static () => FunctionCompletedText))
            .IsEqualTo("fn-next:42");
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept<string>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept(null!, static ex => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept(static value => { }, null!, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept(static value => { }, static ex => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept(null!, static ex => ex.Message, static () => "done"));
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept(static value => value.ToString(), null!, static () => "done"));
        _ = Assert.Throws<ArgumentNullException>(() => next.Accept(static value => value.ToString(), static ex => ex.Message, null!));
        _ = next.ToObservable().Subscribe(observableValues.Add, static ex => throw ex, () => observableCompleted++);
        await Assert.That(observableValues.SequenceEqual(ExpectedObservableValues)).IsTrue();
        await Assert.That(observableCompleted).IsEqualTo(1);
        await Assert.That(observer.Events).Contains("next:42");
    }

    /// <summary>Exercises on-error spark exception, equality, and accept overloads.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SparkOnErrorCoversExceptionEqualityAndAcceptOverloads()
    {
        var next = Spark.CreateOnNext(FortyTwo);
        InvalidOperationException error = new("spark-error");
        var errorSpark = Spark.CreateOnError<int>(error);
        var sameError = Spark.CreateOnError<int>(error);
        RecordingResultWitness<int> observer = new();
        await Assert.That(errorSpark == sameError).IsTrue();
        await Assert.That(errorSpark != next).IsTrue();
        await Assert.That(errorSpark.HasValue).IsFalse();
        await Assert.That(errorSpark.Exception).IsEqualTo(error);
        await Assert.That(errorSpark.Kind).IsEqualTo(SparkKind.OnError);
        await Assert.That(errorSpark.Value).IsEqualTo(0);
        await Assert.That(
                errorSpark.ToString().Contains(nameof(InvalidOperationException), StringComparison.Ordinal))
            .IsTrue();
        await Assert.That(sameError.GetHashCode()).IsEqualTo(errorSpark.GetHashCode());
        errorSpark.Accept((IObserver<int>)observer);
        await Assert.That(errorSpark.Accept<string>(observer)).IsEqualTo("error:spark-error");
        errorSpark.Accept(
            value => observer.Events.Add(value.ToString()),
            ex => observer.Events.Add($"delegate-error:{ex.Message}"),
            () => observer.Events.Add(DelegateCompletedText));
        var errorResult = errorSpark.Accept(
            static value => value.ToString(),
            static ex => $"fn-error:{ex.Message}",
            static () => FunctionCompletedText);
        await Assert.That(errorResult).IsEqualTo("fn-error:spark-error");
        _ = Assert.Throws<ArgumentNullException>(static () => Spark.CreateOnError<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => errorSpark.Accept((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => errorSpark.Accept<string>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(null!, static ex => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(static value => { }, null!, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(static value => { }, static ex => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(() => errorSpark.Accept(null!, static ex => ex.Message, static () => "done"));
        _ = Assert.Throws<ArgumentNullException>(() =>
            errorSpark.Accept(static value => value.ToString(), null!, static () => "done"));
        _ = Assert.Throws<ArgumentNullException>(() =>
            errorSpark.Accept(static value => value.ToString(), static ex => ex.Message, null!));
        await Assert.That(observer.Events).Contains("error:spark-error");
    }

    /// <summary>Exercises on-completed spark equality, accept overloads, and observable validation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SparkOnCompletedCoversEqualityAndAcceptOverloads()
    {
        var completed = Spark.CreateOnCompleted<int>();
        var completedAgain = Spark.CreateOnCompleted<int>();
        RecordingResultWitness<int> observer = new();
        await Assert.That(completed == completedAgain).IsTrue();
        await Assert.That(completed.Equals(completedAgain)).IsTrue();
        await Assert.That(completed.HasValue).IsFalse();
        await Assert.That(completed.Kind).IsEqualTo(SparkKind.OnCompleted);
        await Assert.That(completed.Value).IsEqualTo(0);
        await Assert.That(completed.ToString()).IsEqualTo("OnCompleted()");
        completed.Accept((IObserver<int>)observer);
        await Assert.That(completed.Accept<string>(observer)).IsEqualTo(CompletedText);
        completed.Accept(
            value => observer.Events.Add(value.ToString()),
            ex => observer.Events.Add(ex.Message),
            () => observer.Events.Add(DelegateCompletedText));
        await Assert.That(completed.Accept(static value => value.ToString(), static ex => ex.Message, static () => FunctionCompletedText))
            .IsEqualTo(FunctionCompletedText);
        _ = Assert.Throws<ArgumentNullException>(() => completed.Accept((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => completed.Accept<string>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => completed.Accept(null!, static ex => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => completed.Accept(static value => { }, null!, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => completed.Accept(static value => { }, static ex => { }, null!));
        _ = Assert.Throws<ArgumentNullException>(() => completed.Accept(null!, static ex => ex.Message, static () => "done"));
        _ = Assert.Throws<ArgumentNullException>(() =>
            completed.Accept(static value => value.ToString(), null!, static () => "done"));
        _ = Assert.Throws<ArgumentNullException>(() =>
            completed.Accept(static value => value.ToString(), static ex => ex.Message, null!));
        _ = Assert.Throws<ArgumentNullException>(() => completed.ToObservable(null!));
        await Assert.That(observer.Events).Contains(CompletedText);
    }

    /// <summary>Verifies a completed spark hashes without a value or an exception to reach for.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletedSparksWithNoValueOrExceptionHashEqual()
    {
        var completed = Spark.CreateOnCompleted<int>();
        var sameCompleted = Spark.CreateOnCompleted<int>();
        await Assert.That(completed.GetHashCode()).IsEqualTo(sameCompleted.GetHashCode());
    }

    /// <summary>Records observer events and result values.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingResultWitness<T> : IObserver<T>, IObserver<T, string>
    {
        /// <summary>Prefix of a recorded error event.</summary>
        private const string ErrorPrefix = "error:";

        /// <summary>Prefix of a recorded value event.</summary>
        private const string NextPrefix = "next:";

        /// <summary>Gets the recorded events.</summary>
        public List<string> Events { get; } = [];

        /// <summary>Records completion.</summary>
        public void OnCompleted() => Events.Add(CompletedText);

        /// <summary>Records an error.</summary>
        /// <param name="error">The observed error.</param>
        public void OnError(Exception error) => Events.Add(ErrorPrefix + error.Message);

        /// <summary>Records a next value.</summary>
        /// <param name="value">The observed value.</param>
        public void OnNext(T value) => Events.Add(NextPrefix + value);

        /// <summary>Records completion and returns a result.</summary>
        /// <returns>The completion result.</returns>
        string IObserver<T, string>.OnCompleted()
        {
            Events.Add(CompletedText);
            return CompletedText;
        }

        /// <summary>Records an error and returns a result.</summary>
        /// <param name="exception">The observed error.</param>
        /// <returns>The error result.</returns>
        string IObserver<T, string>.OnError(Exception exception)
        {
            Events.Add(ErrorPrefix + exception.Message);
            return ErrorPrefix + exception.Message;
        }

        /// <summary>Records a value and returns a result.</summary>
        /// <param name="value">The observed value.</param>
        /// <returns>The value result.</returns>
        string IObserver<T, string>.OnNext(T value)
        {
            Events.Add(NextPrefix + value);
            return NextPrefix + value;
        }
    }
}
