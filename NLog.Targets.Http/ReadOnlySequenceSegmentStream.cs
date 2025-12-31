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

internal sealed class ReadOnlySequenceSegmentStream(ReadOnlySequenceSegment<byte> memorySequence) : Stream
{
    private static readonly ReadOnlyMemory<byte> NewLine = new([(byte)'\n']);
    private ReadOnlySequenceSegment<byte>? memorySequence = memorySequence;

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
    {
        var writer = System.IO.Pipelines.PipeWriter.Create(destination, new(minimumBufferSize: bufferSize, leaveOpen: true));

        int written = 0;
        while (memorySequence != null)
        {
            var memory = memorySequence.Memory;
            Debug.WriteLine(System.Text.UTF8Encoding.UTF8.GetString(memorySequence.Memory.Span));
            var r = await WriteCore(writer, memory, cancellationToken).ConfigureAwait(false);
            if (r.IsCanceled || r.IsCompleted)
            {
                return;
            }

            written += memory.Length;

            Next();

            if (memorySequence != null)
            {
                r = await WriteCore(writer, NewLine, cancellationToken).ConfigureAwait(false);
                if (r.IsCanceled || r.IsCompleted)
                {
                    return;
                }

                written += NewLine.Length;
            }
        }

        _ = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static ValueTask<FlushResult> WriteCore(PipeWriter writer, ReadOnlyMemory<byte> memory, CancellationToken cancellationToken) =>
        writer.WriteAsync(memory, cancellationToken);

    private void Next()
    {
        memorySequence = memorySequence?.Next;
    }

    public override void Flush()
    {
    }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
