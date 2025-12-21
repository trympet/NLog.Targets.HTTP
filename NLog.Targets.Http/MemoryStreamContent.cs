using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace NLog.Targets.Http;

internal sealed class MemoryStreamContent : HttpContent
{
    private readonly int _length;
    private ReadOnlySequenceSegment<byte>? memorySequence;

    public MemoryStreamContent(ReadOnlySequenceSegment<byte> memorySequence, int length)
    {
        _length = length;
        this.memorySequence = memorySequence;
    }

    protected sealed override void SerializeToStream(Stream stream, TransportContext? context, CancellationToken cancellationToken)
    {
        base.SerializeToStream(stream, context, cancellationToken);
    }

    protected sealed override Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken)
    {
        return base.CreateContentReadStreamAsync(cancellationToken);
    }

    protected sealed override Task<Stream> CreateContentReadStreamAsync()
    {
        return base.CreateContentReadStreamAsync();
    }

    protected sealed override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
    {
        // Make sure not to get to the Large Object Heap.
        const int LohSizeLimit = 81920;
        int bufferSize = Math.Min(LohSizeLimit, _length);
        var writer = System.IO.Pipelines.PipeWriter.Create(stream, new(minimumBufferSize: bufferSize, leaveOpen: true));

        int written = 0;
        while (memorySequence != null)
        {
            var memory = memorySequence.Memory;
            Debug.Assert(written + memory.Length <= _length);
            var r = await WriteCore(writer, memory, cancellationToken).ConfigureAwait(false);
            if (r.IsCanceled)
            {
                return;
            }

            written += memory.Length;
            Debug.Assert(written <= _length);

            Next();
        }

        _ = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

    }

    protected sealed override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
    {
        return Task.CompletedTask;
    }

    protected sealed override bool TryComputeLength(out long length)
    {
        length = _length;
        return true;
    }

    private static ValueTask<FlushResult> WriteCore(PipeWriter writer, ReadOnlyMemory<byte> memory, CancellationToken cancellationToken) => writer.WriteAsync(memory, cancellationToken);

    private void Next()
    {
        memorySequence = memorySequence?.Next;
    }
}
