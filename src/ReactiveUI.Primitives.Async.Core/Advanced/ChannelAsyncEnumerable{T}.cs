// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Adapts an <see cref="IObservableAsync{T}"/> into an <see cref="IAsyncEnumerable{T}"/> backed by a per-enumeration <see cref="Channel{T}"/>.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ChannelAsyncEnumerable: Source = {_source}")]
public sealed class ChannelAsyncEnumerable<T> : IAsyncEnumerable<T>
{
    /// <summary>The upstream asynchronous observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>Factory producing the buffer channel per enumeration.</summary>
    private readonly Func<Channel<T>> _channelFactory;

    /// <summary>Optional error-resume callback.</summary>
    private readonly Func<Exception, CancellationToken, ValueTask>? _onErrorResume;

    /// <summary>Initializes a new instance of the <see cref="ChannelAsyncEnumerable{T}"/> class.</summary>
    /// <param name="source">The upstream asynchronous observable.</param>
    /// <param name="channelFactory">Factory producing the buffer channel per enumeration.</param>
    public ChannelAsyncEnumerable(IObservableAsync<T> source, Func<Channel<T>> channelFactory)
        : this(source, channelFactory, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ChannelAsyncEnumerable{T}"/> class.</summary>
    /// <param name="source">The upstream asynchronous observable.</param>
    /// <param name="channelFactory">Factory producing the buffer channel per enumeration.</param>
    /// <param name="onErrorResume">Optional resumable error handler.</param>
    public ChannelAsyncEnumerable(
        IObservableAsync<T> source,
        Func<Channel<T>> channelFactory,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(channelFactory);
        _source = source;
        _channelFactory = channelFactory;
        _onErrorResume = onErrorResume;
    }

    /// <summary>Subscribes <paramref name="channel"/> to <paramref name="source"/>, completing the writer on source termination.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="channel">The channel receiving elements.</param>
    /// <param name="onErrorResume">Optional resumable error handler.</param>
    /// <param name="cancellationToken">A token that cancels the subscription.</param>
    /// <returns>The source subscription handle.</returns>
    public static ValueTask<IAsyncDisposable> SubscribeToChannelAsync(
        IObservableAsync<T> source,
        Channel<T> channel,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(channel);

        var onErrorResumeAsync = onErrorResume ?? ((e, _) =>
        {
            channel.Writer.Complete(e);
            return default;
        });

        return CallbackWitnessAsync<T>.SubscribeAsync(
            source,
            channel.Writer.WriteAsync,
            onErrorResumeAsync,
            result =>
            {
                channel.Writer.Complete(result.Exception);
                return default;
            },
            cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        EnumerateCoreAsync(_source, _channelFactory, _onErrorResume, cancellationToken);

    /// <summary>Reads all values from <paramref name="source"/> through a channel created by <paramref name="channelFactory"/>.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="channelFactory">Factory producing the buffer channel.</param>
    /// <param name="onErrorResume">Optional resumable error handler.</param>
    /// <param name="cancellationToken">Enumerator cancellation token.</param>
    /// <returns>An async enumerator over the channel elements.</returns>
    [ExcludeFromCodeCoverage]
    private static async IAsyncEnumerator<T> EnumerateCoreAsync(
        IObservableAsync<T> source,
        Func<Channel<T>> channelFactory,
        Func<Exception, CancellationToken, ValueTask>? onErrorResume,
        CancellationToken cancellationToken)
    {
        var channel = channelFactory();
        var subscription = await SubscribeToChannelAsync(source, channel, onErrorResume, cancellationToken)
            .ConfigureAwait(false);
        try
        {
            await foreach (var x in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return x;
            }
        }
        finally
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
        }
    }
}
