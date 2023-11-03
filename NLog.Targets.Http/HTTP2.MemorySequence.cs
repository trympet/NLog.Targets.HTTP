using System.Buffers;
#if (NETCORE30 || NETSTANDARD21)
using System.Net.Security;
#endif

namespace NLog.Targets.Http
{
    public partial class HttpLogger
    {
        private sealed class MemorySequence : ReadOnlySequenceSegment<byte>
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
    }
}
