// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Awaitable command execution result that avoids allocating a completed task for synchronous commands.</summary>
/// <typeparam name="TResult">The command result type.</typeparam>
public readonly record struct CommandExecution<TResult>
{
    /// <summary>Asynchronous execution task, when execution did not complete synchronously.</summary>
    private readonly Task<TResult>? _task;

    /// <summary>Synchronous result.</summary>
    private readonly TResult? _result;

    /// <summary>Synchronous exception.</summary>
    private readonly Exception? _exception;

    /// <summary>Captured-context preference for task continuations.</summary>
    private readonly bool _continueOnCapturedContext;

    /// <summary>Initializes a new instance of the <see cref="CommandExecution{TResult}"/> struct.</summary>
    /// <param name="result">Synchronous result.</param>
    public CommandExecution(TResult result)
    {
        _task = null;
        _result = result;
        _exception = null;
        _continueOnCapturedContext = true;
    }

    /// <summary>Initializes a new instance of the <see cref="CommandExecution{TResult}"/> struct.</summary>
    /// <param name="task">Asynchronous execution task.</param>
    public CommandExecution(Task<TResult> task)
    {
        _task = task ?? throw new ArgumentNullException(nameof(task));
        _result = default;
        _exception = null;
        _continueOnCapturedContext = true;
    }

    /// <summary>Initializes a new instance of the <see cref="CommandExecution{TResult}"/> struct.</summary>
    /// <param name="exception">Synchronous exception.</param>
    public CommandExecution(Exception exception)
    {
        _task = null;
        _result = default;
        _exception = exception ?? throw new ArgumentNullException(nameof(exception));
        _continueOnCapturedContext = true;
    }

    /// <summary>Initializes a new instance of the <see cref="CommandExecution{TResult}"/> struct.</summary>
    /// <param name="task">Asynchronous execution task.</param>
    /// <param name="result">Synchronous result.</param>
    /// <param name="exception">Synchronous exception.</param>
    /// <param name="continueOnCapturedContext">Captured-context preference.</param>
    private CommandExecution(Task<TResult>? task, TResult? result, Exception? exception, bool continueOnCapturedContext)
    {
        _task = task;
        _result = result;
        _exception = exception;
        _continueOnCapturedContext = continueOnCapturedContext;
    }

    /// <summary>Configures whether task continuations marshal back to the captured context.</summary>
    /// <param name="continueOnCapturedContext">Whether to continue on the captured context.</param>
    /// <returns>A configured command execution awaitable.</returns>
    public CommandExecution<TResult> ConfigureAwait(bool continueOnCapturedContext) =>
        new(_task, _result, _exception, continueOnCapturedContext);

    /// <summary>Gets the awaiter.</summary>
    /// <returns>The awaiter.</returns>
    public Awaiter GetAwaiter() => new(_task, _result, _exception, _continueOnCapturedContext);

    /// <summary>Awaiter for command execution.</summary>
    public readonly record struct Awaiter : ICriticalNotifyCompletion
    {
        /// <summary>Asynchronous execution task.</summary>
        private readonly Task<TResult>? _task;

        /// <summary>Synchronous result.</summary>
        private readonly TResult? _result;

        /// <summary>Synchronous exception.</summary>
        private readonly Exception? _exception;

        /// <summary>Captured-context preference.</summary>
        private readonly bool _continueOnCapturedContext;

        /// <summary>Initializes a new instance of the <see cref="Awaiter"/> struct.</summary>
        /// <param name="task">Asynchronous execution task.</param>
        /// <param name="result">Synchronous result.</param>
        /// <param name="exception">Synchronous exception.</param>
        /// <param name="continueOnCapturedContext">Captured-context preference.</param>
        public Awaiter(Task<TResult>? task, TResult? result, Exception? exception, bool continueOnCapturedContext)
        {
            _task = task;
            _result = result;
            _exception = exception;
            _continueOnCapturedContext = continueOnCapturedContext;
        }

        /// <summary>Gets a value indicating whether the command execution is complete.</summary>
        public bool IsCompleted => _task?.IsCompleted != false;

        /// <summary>Gets the command result or rethrows the command exception.</summary>
        /// <returns>The command result.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Concurrency",
            "PSH1315:A blocking wait on an awaitable that may not be done",
            Justification =
                "Awaiter GetResult must be synchronous; it runs only after completion and unwraps exceptions without AggregateException wrapping.")]
        public TResult GetResult()
        {
            if (_task is not null)
            {
                return _task.GetAwaiter().GetResult();
            }

            if (_exception is not null)
            {
                ExceptionDispatchInfo.Capture(_exception).Throw();
            }

            return _result!;
        }

        /// <inheritdoc/>
        public void OnCompleted(Action continuation) =>
            _task!.ConfigureAwait(_continueOnCapturedContext).GetAwaiter().OnCompleted(continuation);

        /// <inheritdoc/>
        public void UnsafeOnCompleted(Action continuation) =>
            _task!.ConfigureAwait(_continueOnCapturedContext).GetAwaiter().UnsafeOnCompleted(continuation);
    }
}
