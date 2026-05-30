// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

[assembly: TestExecutorAttribute<ReactiveUI.Primitives.Async.Tests.UnhandledExceptionTestExecutor>]
[assembly: NotInParallelAttribute]

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Test executor that saves and restores the <see cref="UnhandledExceptionHandler"/> global state
/// around each test, ensuring test isolation for tests that register custom handlers.
/// </summary>
public sealed class UnhandledExceptionTestExecutor : ITestExecutor
{
    /// <inheritdoc/>
    public async ValueTask ExecuteTest(TestContext context, Func<ValueTask> action)
    {
        var previousHandler = UnhandledExceptionHandler.CurrentHandler;
        try
        {
            await action();
        }
        finally
        {
            UnhandledExceptionHandler.Register(previousHandler);
        }
    }
}
