using NLog.Common;
using NLog.Layouts;
using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NLog.Targets.Http
{
    public partial class HTTP2
    {
        private class LogEvent : IThreadPoolWorkItem
        {
            private readonly HTTP2 _http;
            private AsyncLogEventInfo _asyncLogEventInfo;
            private StringBuilder? sb;

            public LogEvent(HTTP2 http, AsyncLogEventInfo asyncLogEventInfo)
            {
                Debug.Assert(http._state != null);
                _http = http;
                _asyncLogEventInfo = asyncLogEventInfo;
            }

            public virtual Memory<byte> Data { get; private set; }

            public int UncompressedSize { get; set; }

            private State State => _http._state!;

            private Layout Layout => _http.Layout;

            public string GetMessage()
            {
                if (!_http.InMemoryCompression)
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
                    lock (HTTP2.State.StringBuilders)
                    {
                        sb = HTTP2.State.StringBuilders.Count > 0 ? HTTP2.State.StringBuilders.Pop() : null;
                        if (sb == null)
                        {
                            sb = new StringBuilder();
                        }
                    }
                    try
                    {
                        Layout.Render(_asyncLogEventInfo.LogEvent, sb);
                        var buffer = new ArrayBufferWriter<byte>();
                        Build(buffer);
                        Data = MemoryMarshal.AsMemory(buffer.WrittenMemory);
                    }
                    finally
                    {
                        lock (HTTP2.State.StringBuilders)
                        {
                            HTTP2.State.StringBuilders.Push(sb.Clear());
                            sb = null;
                        }
                    }

                    _asyncLogEventInfo.Continuation(null);
                    _asyncLogEventInfo = default;

                    var message = this;
                    if (State.Messages.Count > _http.BatchSize * 2 && _http.TempFile != null)
                    {
                        // Serialize to file if arbitrary event threshold reached.
                        try
                        {
                            message = await SerializedLogEvent.CreateAsync(this, _http.TempFile, _http.TempFileLock, State.Cts.Token);
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
                catch (Exception ex)
                {
                    _asyncLogEventInfo.Continuation(exception: ex);
                }
            }

            private void Build(ArrayBufferWriter<byte> data)
            {
                Debug.Assert(sb != null);
                int maxBytesForCharPos = Encoding.UTF8.GetMaxByteCount(sb.Length);
                Span<byte> byteBuffer = maxBytesForCharPos <= 16384 ? // arbitrary threshold
                    stackalloc byte[maxBytesForCharPos] :
                    new byte[maxBytesForCharPos];
                using var encoder = new BrotliEncoder();
                foreach (var chunk in sb.GetChunks())
                {
                    int count = Encoding.UTF8.GetBytes(chunk.Span, byteBuffer);
                    Debug.Assert(count > 0);
                    UncompressedSize += count;
                    if (_http.InMemoryCompression)
                    {
                        var didCompress = encoder.Compress(byteBuffer[..count], byteBuffer, out _, out count, false);
                        Debug.Assert(didCompress is OperationStatus.Done or OperationStatus.NeedMoreData);
                    }
                    data.Write(byteBuffer[..count]);
                }
                if (_http.InMemoryCompression)
                {
                    var didCompress = encoder.Compress(default, byteBuffer, out _, out var count, isFinalBlock: true);
                    Debug.Assert(didCompress is OperationStatus.Done or OperationStatus.NeedMoreData);
                    data.Write(byteBuffer[..count]);
                }
            }

            private sealed class SerializedLogEvent : LogEvent
            {
                private readonly string path;
                private readonly int offset;
                private readonly int count;

                public SerializedLogEvent(HTTP2 http, string path, int offset, int count)
                    : base(http, default)
                {
                    this.path = path;
                    this.offset = offset;
                    this.count = count;
                }

                public override Memory<byte> Data => Read(path, offset, count);

                public static async Task<SerializedLogEvent> CreateAsync(LogEvent other, string filePath, object writeLock, CancellationToken cancellationToken)
                {
                    var data = other.Data;
                    Debug.Assert(!data.IsEmpty);
                    var offset = await WriteAsync(filePath, data, writeLock, cancellationToken);
                    return new SerializedLogEvent(other._http, filePath, (int)offset, data.Length);
                }

                private static async Task<int> WriteAsync(string file, Memory<byte> data, object truncateLock, CancellationToken cancellationToken)
                {
                    FileStream stream;
                    int offset;
                    lock (truncateLock)
                    {
                        stream = File.Open(file, new FileStreamOptions
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
                        using var stream = File.Open(file, new FileStreamOptions
                        {
                            BufferSize = 0,
                            Access = FileAccess.Read,
                            Mode = FileMode.Open,
                            Share = FileShare.ReadWrite,
                            Options = FileOptions.RandomAccess
                        });
                        byte[] buffer = new byte[count];
                        stream.Position = offset;
                        stream.Read(buffer);
                        return buffer;
                    }
                    catch (Exception)
                    {
                        return Array.Empty<byte>();
                    }
                }
            }
        }
    }
}
