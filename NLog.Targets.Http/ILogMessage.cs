using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text.Json;

namespace NLog.Targets.Http;

public interface ILogMessage
{
    void Serialize(Utf8JsonWriter writer, string category, LogLevel logLevel, string message, Exception? exception, IEnumerable<KeyValuePair<string, object?>>? eventProperties);
}
