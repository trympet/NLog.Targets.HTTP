using System.Buffers;

namespace NLog.Targets.Http;

internal sealed class MemorySequence : ReadOnlySequenceSegment<byte>
{
    public MemorySequence(LogEvent logEvent, MemorySequence? previous, ref MemorySequence? head)
    {
        Memory = logEvent.Data;
        LogEvent = logEvent;
        if (previous != null)
        {
            RunningIndex = previous.RunningIndex + previous.Memory.Length;
            previous.Next = this;
        }
        else
        {
            head = this;
        }
    }

    public LogEvent LogEvent { get; }
}
