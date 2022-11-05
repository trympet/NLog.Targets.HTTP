using System;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
#if (NETCORE30 || NETSTANDARD21)
using System.Net.Security;
#endif

namespace NLog.Targets.Http
{
    internal sealed class ValueTaskSource<T> : IValueTaskSource<T>
    {
        private ManualResetValueTaskSourceCore<T> _mrvtsc;
        private CancellationTokenRegistration _ctr;
        private bool _completed;
        private bool _stopping;
        private bool _waiting;
        private T? _result;

        public ValueTask<T> WaitAsync(CancellationToken cancellationToken)
        {
            lock (this)
            {
                Debug.Assert(!_waiting);
                if (cancellationToken.IsCancellationRequested)
                {
                    return ValueTask.FromCanceled<T>(cancellationToken);
                }

                if (_completed)
                {
                    _completed = false;
                    var result = _result;
                    _result = default;
                    return ValueTask.FromResult(result!);
                }

                _waiting = true;
                _ctr = cancellationToken.UnsafeRegister(static (state, cancellationToken) => ((ValueTaskSource<T>)state!).SetCancelled(cancellationToken: cancellationToken), this);
                return new ValueTask<T>(this, _mrvtsc.Version);
            }
        }

        public void SetCancelled(CancellationToken cancellationToken)
        {
            bool completeTask = false;
            lock (this)
            {
                if (_waiting && !_completed)
                {
                    completeTask = true;
                    _stopping = true;
                }
            }
            if (completeTask)
            {
                _mrvtsc.SetException(ExceptionDispatchInfo.SetCurrentStackTrace(new OperationCanceledException(cancellationToken)));
            }
        }

        public void SetResult(T result)
        {
            lock (this)
            {
                if (_stopping)
                {
                    return;
                }

                _completed = true;
                if (!_waiting)
                {
                    _result = result;
                    return;
                }
            }

            _mrvtsc.SetResult(result);
        }

        public T GetResult(short token)
        {
            _ctr.Dispose();
            lock (this)
            {
                try
                {
                    return _mrvtsc.GetResult(token);
                }
                finally
                {
                    _mrvtsc.Reset();
                    _ctr = default;
                    _completed = false;
                    _result = default;
                    _waiting = false;
                    _stopping = false;
                }
            }
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            return _mrvtsc.GetStatus(token);
        }

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _mrvtsc.OnCompleted(continuation, state, token, flags);
        }
    }
}
