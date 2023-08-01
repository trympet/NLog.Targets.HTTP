using NLog.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
#if (NETCORE30 || NETSTANDARD21)
using System.Net.Security;
#endif

namespace NLog.Targets.Http
{
    [Target("HTTP")]
    public partial class HTTP2 : TargetWithLayout
    {
        public static Func<HttpMessageHandler>? Handler { get; set; }

        private static readonly Dictionary<string, HttpMethod> AvailableHttpMethods = new Dictionary<string, HttpMethod>
            {{"post", HttpMethod.Post}, {"get", HttpMethod.Get}};
        private State? _state;
        private Task? _worker;
        private string _proxyPassword = string.Empty;
        private string _proxyUrl = string.Empty;
        private string _proxyUser = string.Empty;
        private string _url = string.Empty;
        private string _accept = "application/json";
        private string? _authorization;
        private int _connectTimeout = 30000;
        private bool _expect100Continue = ServicePointManager.Expect100Continue;
        private bool _ignoreSslErrors = true;
        private HttpStatusCode _phaseStatus;

        public static EventHandler<FlushErrorEventArgs>? FlushError;
        private string? tempDir;

        public int HttpErrorRetryTimeout { get; set; } = 500;
        public bool InMemoryCompression { get; set; } = true;
        public int MessagePollInterval { get; set; } = 3000;
        public int TooManyRequestsTimeout { get; set; } = 750;
        public int HttpErrorDelay { get; set; }
        public string Url { get; set; } = string.Empty; public string ProxyUrl
        {
            get => _proxyUrl;
            set
            {
                if (value == _proxyUrl) return;
                _proxyUrl = value;
                //NotifyPropertyChanged(nameof(ProxyUrl));
            }
        }

        public string ProxyUser
        {
            get => _proxyUser;
            set
            {
                if (value == _proxyUser) return;
                _proxyUser = value;
                //NotifyPropertyChanged(nameof(ProxyUser));
            }
        }

        public string ProxyPassword
        {
            get => _proxyPassword;
            set
            {
                if (value == _proxyPassword) return;
                _proxyPassword = value;
                //NotifyPropertyChanged(nameof(ProxyPassword));
            }
        }

        public int BatchSize { get; set; } = 64;

        public string Method { get; set; } = "POST";

        public string? Authorization
        {
            get => _authorization;
            set
            {
                if (value == _authorization) return;
                _authorization = value;
                //NotifyPropertyChanged(nameof(Authorization));
            }
        }

        public bool Expect100Continue
        {
            get => _expect100Continue;
            set
            {
                if (value == _expect100Continue) return;
                _expect100Continue = value;
                //NotifyPropertyChanged(nameof(Expect100Continue));
            }
        }

        public int ConnectTimeout
        {
            get => _connectTimeout;
            set
            {
                if (value == _connectTimeout) return;
                _connectTimeout = value;
                //NotifyPropertyChanged(nameof(ConnectTimeout));
            }
        }

        public bool IgnoreSslErrors
        {
            get => _ignoreSslErrors;
            set
            {
                if (value == _ignoreSslErrors) return;
                _ignoreSslErrors = value;
                //NotifyPropertyChanged(nameof(IgnoreSslErrors));
            }
        }

        public string Accept
        {
            get => _accept;
            set
            {
                if (value == _accept) return;
                _accept = value;
                //NotifyPropertyChanged(nameof(Accept));
            }
        }

        /// <summary>
        /// Gets or sets the temporary directory used when
        /// </summary>
        public string? TempDir
        {
            get => tempDir;
            set
            {
                tempDir = value;
                TempFile = value != null
                    ? Path.Combine(value, Guid.NewGuid().ToString())
                    : null;
            }
        }

        internal string? TempFile { get; set; }
        internal object TempFileLock { get; } = new object();

        protected override void InitializeTarget()
        {
            if (_worker != null)
            {
                throw new InvalidOperationException("The target is already running.");
            }

            base.InitializeTarget();
            _state = new(this);
            _state.HttpClientRef = HttpClientPool.Instance.Aquire(this, out _state.HttpClient);
            _worker = Worker(_state.Cts.Token);
        }

        protected override void CloseTarget()
        {
            base.CloseTarget();
            Debug.Assert(_state != null);
            _state.Cts.Cancel();
            if (TempFile != null)
            {
                File.Delete(TempFile);
            }
        }

        protected override async void FlushAsync(AsyncContinuation asyncContinuation)
        {
            try
            {
                Debug.Assert(_state != null);
                var tcs = new TaskCompletionSource();
                ThreadPool.UnsafeRegisterWaitForSingleObject(
                    _state.PhaseComplete,
                    static (x, _) => ((TaskCompletionSource)x!).SetResult(),
                    tcs,
                    millisecondsTimeOutInterval: -1,
                    executeOnlyOnce: true
                );
                // Complete 1 phase
                for (int i = 0; i < BatchSize; i++)
                {
                    _state.PendingMessages.Release();
                }
                await tcs.Task;
                if (_phaseStatus != HttpStatusCode.OK)
                {
                    var sb = new StringBuilder();
                    while (_state.Messages.TryTake(out var message))
                    {
                        sb.Append(message.GetMessage());
                    }
                    FlushError?.Invoke(this, new(sb.ToString()));
                }
                asyncContinuation(null);
            }
            catch (Exception e)
            {
                asyncContinuation(e);
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }

        protected override void WriteAsyncThreadSafe(AsyncLogEventInfo logEvent)
        {
            Debug.Assert(_state != null);
            ThreadPool.UnsafeQueueUserWorkItem(new LogEvent(this, logEvent), false);
        }

        /// <summary>
        /// If <see cref="MessagePollInterval"/> has elapsed, and there are pending messages.
        /// if the number of pending messages exceeds max queue size.
        /// </summary>
        /// <returns></returns>
        private async Task Worker(CancellationToken cancellationToken)
        {
            int pendingCount = 0;
            int batchSize = BatchSize;
            int wait = -1;
            var valueTaskSource = new ValueTaskSource<bool>();
            Stopwatch phaseDuration = new Stopwatch();
            while (true)
            {
                bool signal = false;
                if (wait != 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var registration = ThreadPool.UnsafeRegisterWaitForSingleObject(
                        _state!.PendingMessages,
                        static (state, timedOut) => ((ValueTaskSource<bool>)state!).SetResult(!timedOut),
                        valueTaskSource,
                        wait,
                        executeOnlyOnce: true
                    );
                    try
                    {
                        signal = await valueTaskSource.WaitAsync(cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        var didUnregister = registration.Unregister(null);
                        Debug.Assert(didUnregister);
                        throw;
                    }
                }
                if (pendingCount == 0)
                {
                    // start of phase.
                    phaseDuration.Start();
                    wait = MessagePollInterval;
                    pendingCount = 1;
                }
                else if ((pendingCount + 1) % batchSize == 0 || !signal)
                {
                    // end of phase. send http request.
                    wait = -1;
                    phaseDuration.Reset();
                    pendingCount = 0;
                    _phaseStatus = await SendAndConsumeMessages(cancellationToken);
                    _state!.PhaseComplete.Set();
                    switch (_phaseStatus)
                    {
                        case HttpStatusCode.OK:
                            break;
                        case HttpStatusCode.TooManyRequests:
                            await Task.Delay(TooManyRequestsTimeout, cancellationToken);
                            break;
                        default:
                            await Task.Delay(HttpErrorRetryTimeout, cancellationToken);
                            break;
                    }
                }
                else
                {
                    // during phase
                    wait = Math.Max(0, MessagePollInterval - (int)phaseDuration.Elapsed.TotalMilliseconds);
                    pendingCount = (pendingCount + 1) % batchSize;
                }
            }
        }
        private async Task<HttpStatusCode> SendAndConsumeMessages(CancellationToken cancellationToken)
        {
            var head = GetMemorySequence(out var length);
            if (head == null)
                return HttpStatusCode.OK;

            HttpStatusCode result = HttpStatusCode.BadRequest;
            try
            {
                using var request = new HttpRequestMessage
                {
                    Version = new Version(2, 0),
                    Content = InMemoryCompression ? new CompressedMemoryStreamContent(head, length) : new MemoryStreamContent(head, length),
                };

                using var httpResponseMessage = await _state!.HttpClient!.SendAsync(request, cancellationToken).ConfigureAwait(false);
                result = httpResponseMessage.StatusCode;
            }
            catch (HttpRequestException)
            {
            }
            catch (TaskCanceledException)
            {
            }
            finally
            {
                if (result != HttpStatusCode.OK)
                {
                    while (head != null)
                    {
                        _state!.Messages.Add(head.LogEvent);
                        head = head.Next as MemorySequence;
                    }
                }
            }

            return result;
        }

        private MemorySequence? GetMemorySequence(out int length)
        {
            MemorySequence? head = null;
            MemorySequence? sequence = null;
            length = 0;
            int capacity = BatchSize;
            while (_state!.Messages.TryTake(out var message) && capacity-- > 0)
            {
                Debug.Assert(message!.Data.Length > 0);
                sequence = new MemorySequence(message, sequence, ref head);
                length += message.UncompressedSize;
            }

            return head;
        }

        private sealed class State
        {
            internal static readonly Stack<StringBuilder> StringBuilders = new(Environment.ProcessorCount);
            internal readonly ConcurrentBag<LogEvent> Messages = new();
            internal readonly CancellationTokenSource Cts = new();
            /// <summary>
            /// Signals any pending messages.
            /// </summary>
            internal readonly Semaphore PendingMessages;
            internal readonly AutoResetEvent PhaseComplete;
            internal HttpClient? HttpClient;
            internal IDisposable? HttpClientRef;
            public State(HTTP2 http)
            {
                PendingMessages = new Semaphore(0, int.MaxValue);
                PhaseComplete = new AutoResetEvent(false);
            }
        }
    }
}
