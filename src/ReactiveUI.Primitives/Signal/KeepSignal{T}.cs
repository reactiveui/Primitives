// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class KeepSignal<T> : IObservable<T>, IRequireCurrentThread<T>
{
    private readonly IObservable<T> _source;
    private readonly Func<T, bool> _predicate;

    public KeepSignal(IObservable<T> source, Func<T, bool> predicate)
    {
        _source = source;
        _predicate = predicate;
    }

    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        return _source.Subscribe(new KeepObserver(observer, _predicate));
    }

    private sealed class KeepObserver : IObserver<T>
    {
        private readonly IObserver<T> _observer;
        private readonly Func<T, bool> _predicate;
        private bool _stopped;

        public KeepObserver(IObserver<T> observer, Func<T, bool> predicate)
        {
            _observer = observer;
            _predicate = predicate;
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

        public void OnNext(T value)
        {
            if (_stopped)
            {
                return;
            }

            bool keep;
            try
            {
                keep = _predicate(value);
            }
            catch (Exception error)
            {
                OnError(error);
                return;
            }

            if (keep)
            {
                _observer.OnNext(value);
            }
        }
    }
}
