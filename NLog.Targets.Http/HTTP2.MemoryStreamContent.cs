using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
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
    public partial class HttpLogger
    {
        private sealed class CompressedMemoryStreamContent : MemoryStreamContent
        {
            private BrotliDecoder decoder;

            public CompressedMemoryStreamContent(ReadOnlySequenceSegment<byte> memorySequence, int length) : base(memorySequence, length)
            {
            }

            protected override void ReadCore(Span<byte> buffer, ReadOnlySpan<byte> source, out int bytesWritten, out int bytesConsumed)
            {
                var didDecompress = decoder.Decompress(source, buffer, out bytesConsumed, out bytesWritten);
                switch (didDecompress)
                {
                    case OperationStatus.InvalidData:
                    case OperationStatus.NeedMoreData:
                        throw new InvalidDataException();
                    case OperationStatus.DestinationTooSmall:
                        Debug.Assert(bytesConsumed == source.Length);
                        break;
                    default:
                        Debug.Assert(didDecompress == OperationStatus.Done);
                        break;
                }
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                if (disposing)
                {
                    decoder.Dispose();
                }
            }

            protected override void Next()
            {
                base.Next();
                decoder.Dispose();
                decoder = default;
            }
        }

        private class MemoryStreamContent : HttpContent
        {
            private readonly int _length;
            private long _consumed;
            private ReadOnlySequenceSegment<byte>? memorySequence;

            public MemoryStreamContent(ReadOnlySequenceSegment<byte> memorySequence, int length)
            {
                _length = length;
                this.memorySequence = memorySequence;
            }

            protected override void SerializeToStream(Stream stream, TransportContext? context, CancellationToken cancellationToken)
            {
                base.SerializeToStream(stream, context, cancellationToken);
            }

            protected override Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken)
            {
                return base.CreateContentReadStreamAsync(cancellationToken);
            }

            protected override Task<Stream> CreateContentReadStreamAsync()
            {
                return base.CreateContentReadStreamAsync();
            }

            protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
            {
                byte[] buffer = ArrayPool<byte>.Shared.Rent(Math.Min(81920, _length));
                try
                {
                    int bytesRead;
                    while ((bytesRead = Read(buffer)) != 0)
                    {
#if DEBUG
                        var debug = Encoding.UTF8.GetString(new Span<byte>(buffer, 0, bytesRead));
#endif
                        await stream.WriteAsync(new ReadOnlyMemory<byte>(buffer, 0, bytesRead), cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }

            protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
            {
                return Task.CompletedTask;
            }

            protected override bool TryComputeLength(out long length)
            {
                length = _length;
                return true;
            }

            private int Read(Span<byte> buffer)
            {
                int written = 0;
                while (memorySequence != null && buffer.Length > 0)
                {
                    var source = memorySequence.Memory.Span[(int)(_consumed - memorySequence.RunningIndex)..];
                    while (source.Length > 0)
                    {
                        ReadCore(buffer, source, out var bytesWritten, out var bytesConsumed);
                        _consumed += bytesConsumed;
                        written += bytesWritten;
                        source = source[bytesConsumed..];
                        buffer = buffer[bytesWritten..];
                    }
                    Next();
                }

                return written;
            }

            protected virtual void ReadCore(Span<byte> buffer, ReadOnlySpan<byte> source, out int bytesWritten, out int bytesConsumed)
            {
                source.CopyTo(buffer);
                bytesConsumed = bytesWritten = Math.Min(source.Length, buffer.Length);
            }

            protected virtual void Next()
            {
                memorySequence = memorySequence?.Next;
            }
        }
    }
}
