using System;
using System.Collections.Generic;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace NLog.Targets.Http;

public interface ILogMessage
{
    void Serialize(Utf8JsonWriter writer, string category, LogLevel logLevel, int threadId, string message, Exception? exception, IEnumerable<KeyValuePair<string, object?>>? eventProperties);
}
