using System;
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

        public ValueTask<T> WaitAsync(CancellationToken cancellationToken)
        {
            _ctr = cancellationToken.UnsafeRegister(static (state, cancellationToken) => ((ValueTaskSource<T>)state!).SetCancelled(cancellationToken: cancellationToken), this);
            return new ValueTask<T>(this, _mrvtsc.Version);
        }

        public void SetCancelled(CancellationToken cancellationToken)
        {
            _mrvtsc.SetException(ExceptionDispatchInfo.SetCurrentStackTrace(new OperationCanceledException(cancellationToken)));
        }

        public void SetResult(T result)
        {
            _mrvtsc.SetResult(result);
        }

        public T GetResult(short token)
        {
            _ctr.Dispose();
            try
            {
                return _mrvtsc.GetResult(token);
            }
            finally
            {
                _mrvtsc.Reset();
                _ctr = default;
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
