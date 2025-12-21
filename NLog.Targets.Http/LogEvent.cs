using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NLog.Targets.Http;

internal abstract class LogEvent : IThreadPoolWorkItem
{
    protected LogEvent(HttpLogger http)
    {
        Debug.Assert(http.State != null);
        HttpLogger = http;
    }

    public virtual ReadOnlyMemory<byte> Data { get; private set; }
    public int UncompressedSize { get; private set; }
    private protected HttpLogger HttpLogger { get; }
    private State State => HttpLogger.State!;

    public static LogEvent Create(HttpLogger http, string category, LogLevel logLevel, int threadId, Exception? exception, string message, IEnumerable<KeyValuePair<string, object?>>? eventProperties)
    {
        return new SerializableLogEvent(http, category, logLevel, threadId, exception, message, eventProperties);
    }

    public string GetMessage()
    {
        if (!HttpLogger.InMemoryCompression)
        {
            return Encoding.UTF8.GetString(Data.Span);
        }

        Span<byte> result = new byte[UncompressedSize];
        Span<byte> buffer = result;
        int totalWritten = 0;
        var data = Data.Span;
        using var decoder = new BrotliDecoder();
        OperationStatus s;
        while ((s = decoder.Decompress(data, buffer, out var consumed, out var written)) == OperationStatus.Done && consumed > 0)
        {
            data = data[consumed..];
            buffer = buffer[written..];
            totalWritten += written;
        }

        Debug.Assert(totalWritten == UncompressedSize);
        return Encoding.UTF8.GetString(result[..totalWritten]);
    }

    public async void Execute()
    {
        try
        {
            var ms = new ArrayBufferWriter<byte>();
            using var writer = new Utf8JsonWriter(
                ms,
                new JsonWriterOptions
                {
#if !DEBUG
                SkipValidation = true
#endif
                });
            // TODO: to json in sb
            Serialize(writer);
            writer.Flush();
            {
                UncompressedSize = (int)ms.WrittenCount;
                Data = ms.WrittenMemory;
            }

            var message = this;
            if (State.Messages.Count > HttpLogger.BatchSize * 2 && HttpLogger.TempFile != null)
            {
                // Serialize to file if arbitrary event threshold reached.
                try
                {
                    message = await SerializedLogEvent.CreateAsync(this, HttpLogger.TempFile, HttpLogger.TempFileLock, State.Cts.Token);
                    message.UncompressedSize = (int)ms.WrittenCount;
                }
                catch (Exception e)
                {
                    // Discard the event.
                    Debug.Fail(e.Message);
                }
            }

            State.Messages.Add(message);
            State.PendingMessages.Release();
        }
        catch (ObjectDisposedException)
        {
            // We're shutting down.
        }
        catch (Exception ex)
        {
            Debug.Fail(ex.Message);
        }
    }

    internal abstract void Serialize(Utf8JsonWriter writer);

    private sealed class SerializableLogEvent : LogEvent
    {
        private readonly string category;
        private readonly LogLevel logLevel;
        private readonly int threadId;
        private readonly Exception? exception;
        private readonly IEnumerable<KeyValuePair<string, object?>>? eventProperties;
        private readonly string message;

        public SerializableLogEvent(HttpLogger http, string category, LogLevel logLevel, int threadId, Exception? exception, string message, IEnumerable<KeyValuePair<string, object?>>? eventProperties)
            : base(http)
        {
            this.category = category;
            this.logLevel = logLevel;
            this.threadId = threadId;
            this.exception = exception;
            this.message = message;
            this.eventProperties = eventProperties;
        }

        internal sealed override void Serialize(Utf8JsonWriter writer)
        {
            base.HttpLogger.LogMessage.Serialize(writer, category, logLevel, threadId, message, exception, eventProperties);
        }
    }

    private sealed class SerializedLogEvent : LogEvent
    {
        private readonly string path;
        private readonly int offset;
        private readonly int count;

        public SerializedLogEvent(HttpLogger http, string path, int offset, int count)
            : base(http)
        {
            this.path = path;
            this.offset = offset;
            this.count = count;
        }

        public override ReadOnlyMemory<byte> Data => Read(path, offset, count);

        public static async Task<SerializedLogEvent> CreateAsync(LogEvent other, string filePath, object writeLock, CancellationToken cancellationToken)
        {
            var data = other.Data;
            Debug.Assert(!data.IsEmpty);
            var offset = await WriteAsync(filePath, data, writeLock, cancellationToken);
            return new SerializedLogEvent(other.HttpLogger, filePath, (int)offset, data.Length);
        }

        private static async Task<int> WriteAsync(string file, ReadOnlyMemory<byte> data, object truncateLock, CancellationToken cancellationToken)
        {
            FileStream stream;
            int offset;
            lock (truncateLock)
            {
                stream = File.Open(
                    file,
                    new FileStreamOptions
                    {
                        BufferSize = data.Length,
                        Access = FileAccess.Write,
                        Mode = FileMode.Append,
                        Options = FileOptions.Asynchronous,
                        Share = FileShare.ReadWrite,
                    });
                offset = (int)stream.Length;
                stream.SetLength(stream.Length + data.Length);
            }

            await stream.WriteAsync(data, cancellationToken);
            stream.Dispose();
            return offset;
        }

        private static byte[] Read(string file, int offset, int count)
        {
            try
            {
                using var stream = File.Open(
                    file,
                    new FileStreamOptions
                    {
                        BufferSize = 0,
                        Access = FileAccess.Read,
                        Mode = FileMode.Open,
                        Share = FileShare.ReadWrite,
                        Options = FileOptions.RandomAccess
                    });
                byte[] buffer = new byte[count];
                stream.Position = offset;
                _ = stream.Read(buffer);
                return buffer;
            }
            catch (Exception)
            {
                return [];
            }
        }

        internal override void Serialize(Utf8JsonWriter writer)
        {
            throw new NotImplementedException();
        }
    }
}
