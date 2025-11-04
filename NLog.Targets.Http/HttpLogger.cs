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
using Microsoft.Extensions.Logging;

namespace NLog.Targets.Http;

public sealed class HttpLogger : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly State _state;
    private Task? _worker;
    private HttpStatusCode _phaseStatus;
    private string? _tempDir;

    public HttpLogger(HttpMessageHandler messageHandler, ILogMessage logMessage)
    {
        _httpClient = new HttpClient(messageHandler, false);
        _state = new();
        LogMessage = logMessage;
        _worker = Worker(_state.Token);
    }

    public static event EventHandler<FlushErrorEventArgs>? FlushError;

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
    internal State State => _state;
    internal ILogMessage LogMessage { get; }

    public void Dispose()
    {
        var worker = _worker;
        if (worker is null)
        {
            return;
        }

        worker = Interlocked.CompareExchange(ref _worker, null, worker);
        if (worker is null)
        {
            return;
        }

        _state.Cts.Cancel();
        try
        {
            worker.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
            // HTTP handler disposed, but we're not.
            Trace.TraceWarning($"Dispose {nameof(HttpLogger)} before underlying handler.");
        }

        _httpClient.Dispose();
        _state.Dispose();

        if (TempFile != null)
        {
            try
            {
                File.Delete(TempFile);
            }
            catch (Exception)
            {
                // Best effort.
                Debug.Fail("Failed to delete temp file.");
            }
        }
    }

    public void Log<TState>(string category, LogLevel logLevel, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (_worker is not null)
        {
            var workItem = LogEvent.Create(this, category, logLevel, Environment.CurrentManagedThreadId, exception, formatter(state, exception), state as IEnumerable<KeyValuePair<string, object?>>);
            workItem.Execute();
        }
        else
        {
            Trace.TraceWarning($"Log message after dispose: {formatter(state, exception)}");
        }
    }

    public async Task FlushAsync(TimeSpan timeout)
    {
        bool timedOut = true;
        try
        {
            var tcs = new TaskCompletionSource<bool>();
            _ = ThreadPool.UnsafeRegisterWaitForSingleObject(
                _state.PhaseComplete,
                static (x, timedOut) => ((TaskCompletionSource<bool>)x!).SetResult(timedOut),
                tcs,
                timeout,
                executeOnlyOnce: true
            );

            // Complete 1 phase
            for (int i = 0; i < BatchSize; i++)
            {
                _ = _state.PendingMessages.Release();
            }

            timedOut = await tcs.Task.ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // We're shutting down.
        }
        catch (Exception)
        {
            Debug.Fail("Unhandled error during flush.");
            timedOut = true;
        }

        if (timedOut || _phaseStatus != HttpStatusCode.OK)
        {
            var sb = new StringBuilder();
            while (_state.Messages.TryTake(out var message))
            {
                _ = sb.Append(message.GetMessage());
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
                _ = _state!.PhaseComplete.Set();
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
        {
            return HttpStatusCode.OK;
        }

        HttpStatusCode result = HttpStatusCode.BadRequest;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, requestUri: default(Uri))
            {
                Version = HttpVersion.Version20,
                Content = InMemoryCompression ? new CompressedMemoryStreamContent(head, length) : new MemoryStreamContent(head, length),
            };

            using var httpResponseMessage = await _httpClient!.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            result = httpResponseMessage.StatusCode;
        }
        catch (HttpRequestException)
        {
        }
        catch (TaskCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Swallow cancellations if cancellation isn't signaled.
        }
        catch (ObjectDisposedException ex) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("Underlying handler disposed", ex, cancellationToken);
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
}

internal sealed class State : IDisposable
{
    internal readonly ConcurrentBag<LogEvent> Messages = [];
    internal readonly CancellationTokenSource Cts = new();
    internal readonly CancellationToken Token;

    /// <summary>
    /// Signals any pending messages.
    /// </summary>
    internal readonly Semaphore PendingMessages;
    internal readonly AutoResetEvent PhaseComplete;

    public State()
    {
        PendingMessages = new Semaphore(0, int.MaxValue);
        PhaseComplete = new AutoResetEvent(false);
        Token = Cts.Token;
    }

    public void Dispose()
    {
        Cts.Dispose();
        PendingMessages.Dispose();
        PhaseComplete.Dispose();
    }
}
