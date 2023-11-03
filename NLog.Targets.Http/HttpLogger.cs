using Microsoft.Extensions.Logging;
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

namespace NLog.Targets.Http;

public sealed partial class HttpLogger : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ILogMessage _logMessage;
    private State _state;
    private Task _worker;
    private HttpStatusCode _phaseStatus;
    private string? _tempDir;

    public HttpLogger(HttpMessageHandler messageHandler, ILogMessage logMessage)
    {
        _state = new(this);
        _worker = Worker(_state.Cts.Token);
        _httpClient = new HttpClient(messageHandler);
        _logMessage = logMessage;
    }

    public static EventHandler<FlushErrorEventArgs>? FlushError;

    public HttpClient HttpClient => _httpClient;

    public Uri? Url
    {
        get => _httpClient.BaseAddress;
        set => _httpClient.BaseAddress = value;
    }

    public TimeSpan ConnectTimeout
    {
        get => _httpClient.Timeout;
        set => _httpClient.Timeout = value;
    }

    public int HttpErrorRetryTimeout { get; set; } = 500;
    public bool InMemoryCompression { get; set; } = true;
    public int MessagePollInterval { get; set; } = 3000;
    public int TooManyRequestsTimeout { get; set; } = 750;

    public int BatchSize { get; set; } = 64;

    /// <summary>
    /// Gets or sets the temporary directory used when
    /// </summary>
    public string? TempDir
    {
        get => _tempDir;
        set
        {
            _tempDir = value;
            TempFile = value != null
                ? Path.Combine(value, Guid.NewGuid().ToString())
                : null;
        }
    }

    internal string? TempFile { get; set; }

    internal object TempFileLock { get; } = new object();

    public void Dispose()
    {
        _state.Cts.Cancel();
        if (TempFile != null)
        {
            File.Delete(TempFile);
        }
    }

    public void Log<TState>(string category, LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var workItem = new SerializableLogEvent(this, category, logLevel, eventId, exception, formatter(state, exception), state as IEnumerable<KeyValuePair<string, object?>>);
        ThreadPool.UnsafeQueueUserWorkItem(workItem, false);
    }

    public async Task FlushAsync()
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

            using var httpResponseMessage = await _httpClient!.SendAsync(request, cancellationToken).ConfigureAwait(false);
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
        internal readonly ConcurrentBag<LogEvent> Messages = new();
        internal readonly CancellationTokenSource Cts = new();
        /// <summary>
        /// Signals any pending messages.
        /// </summary>
        internal readonly Semaphore PendingMessages;
        internal readonly AutoResetEvent PhaseComplete;
        public State(HttpLogger http)
        {
            PendingMessages = new Semaphore(0, int.MaxValue);
            PhaseComplete = new AutoResetEvent(false);
        }
    }
}
