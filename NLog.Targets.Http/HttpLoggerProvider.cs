using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NLog.Targets.Http;

public sealed class HttpLoggerProvider : ILoggerProvider
{
    private readonly HttpLogger logger;

    private HttpLoggerProvider(HttpLogger logger, LogLevel minLevel, LogLevel maxLevel)
    {
        this.logger = logger;
        MinLevel = minLevel;
        MaxLevel = maxLevel;
    }
    public LogLevel MinLevel { get; set; }

    public LogLevel MaxLevel { get; set; }

    public static HttpLoggerProvider Create(HttpMessageHandler httpMessageHandler, ILogMessage logMessage, LogLevel minLevel, LogLevel maxLevel = LogLevel.Critical)
    {
        return new HttpLoggerProvider(new HttpLogger(httpMessageHandler, logMessage), minLevel, maxLevel);
    }

    public static HttpLoggerProvider Create(HttpLogger logger, LogLevel minLevel, LogLevel maxLevel = LogLevel.Critical)
    {
        return new HttpLoggerProvider(logger, minLevel, maxLevel);
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new Logger(this, categoryName);
    }

    public Task FlushAsync(TimeSpan timeout) => logger.FlushAsync(timeout);

    public void Dispose()
    {
        logger.Dispose();
    }

    public bool IsEnabled(LogLevel logLevel) => MinLevel <= logLevel && logLevel <= MaxLevel;

    private sealed class Logger : ILogger
    {
        private readonly HttpLoggerProvider httpLoggerProvider;
        private readonly string category;

        public Logger(HttpLoggerProvider httpLoggerProvider, string category)
        {
            this.httpLoggerProvider = httpLoggerProvider;
            this.category = category;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            throw new NotImplementedException();
        }

        public bool IsEnabled(LogLevel logLevel) =>
            httpLoggerProvider.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            httpLoggerProvider.logger.Log(category, logLevel, state, exception, formatter);
        }
    }
}
