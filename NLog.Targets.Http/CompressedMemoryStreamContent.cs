using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;

namespace NLog.Targets.Http;

internal sealed class CompressedMemoryStreamContent : MemoryStreamContent
{
    private BrotliDecoder decoder;

    public CompressedMemoryStreamContent(ReadOnlySequenceSegment<byte> memorySequence, int length)
        : base(memorySequence, length)
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
