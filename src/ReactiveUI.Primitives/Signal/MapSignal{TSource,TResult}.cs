// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class MapSignal<TSource, TResult> : IObservable<TResult>, IRequireCurrentThread<TResult>
{
    private readonly IObservable<TSource> _source;
    private readonly Func<TSource, TResult> _selector;

    public MapSignal(IObservable<TSource> source, Func<TSource, TResult> selector)
    {
        _source = source;
        _selector = selector;
    }

    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<TSource> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        return _source.Subscribe(new MapObserver(observer, _selector));
    }

    private sealed class MapObserver : IObserver<TSource>
    {
        private readonly IObserver<TResult> _observer;
        private readonly Func<TSource, TResult> _selector;
        private bool _stopped;

        public MapObserver(IObserver<TResult> observer, Func<TSource, TResult> selector)
        {
            _observer = observer;
            _selector = selector;
        }

        public void OnCompleted()
        {
            if (!_stopped)
            {
                _stopped = true;
                _observer.OnCompleted();
            }
        }

        public void OnError(Exception error)
        {
            if (!_stopped)
            {
                _stopped = true;
                _observer.OnError(error);
            }
        }

        public void OnNext(TSource value)
        {
            if (_stopped)
            {
                return;
            }

            TResult result;
            try
            {
                result = _selector(value);
            }
            catch (Exception error)
            {
                OnError(error);
                return;
            }

            _observer.OnNext(result);
        }
    }
}
