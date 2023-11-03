using Microsoft.Extensions.Logging;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace NLog.Targets.Http;

public sealed class HttpLoggerProvider : ILoggerProvider
{
    private readonly HttpLogger logger;

    private HttpLoggerProvider(HttpLogger logger, Microsoft.Extensions.Logging.LogLevel minLevel, Microsoft.Extensions.Logging.LogLevel maxLevel)
    {
        this.logger = logger;
        this.MinLevel = minLevel;
        this.MaxLevel = maxLevel;
    }
    public Microsoft.Extensions.Logging.LogLevel MinLevel { get; set; }

    public Microsoft.Extensions.Logging.LogLevel MaxLevel { get; set; }

    public static HttpLoggerProvider Create<T>(HttpMessageHandler httpMessageHandler, ILogMessage logMessage, Microsoft.Extensions.Logging.LogLevel minLevel, Microsoft.Extensions.Logging.LogLevel maxLevel = Microsoft.Extensions.Logging.LogLevel.Critical)
    {
        return new HttpLoggerProvider(new HttpLogger(httpMessageHandler, logMessage), minLevel, maxLevel);
    }

    public static HttpLoggerProvider Create(HttpLogger logger, Microsoft.Extensions.Logging.LogLevel minLevel, Microsoft.Extensions.Logging.LogLevel maxLevel = Microsoft.Extensions.Logging.LogLevel.Critical)
    {
        return new HttpLoggerProvider(logger, minLevel, maxLevel);
    }

    public Microsoft.Extensions.Logging.ILogger CreateLogger(string categoryName)
    {
        return new Logger(this, categoryName);
    }

    public Task FlushAsync() => logger.FlushAsync();

    public void Dispose()
    {
        logger.Dispose();
    }

    public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => this.MinLevel <= logLevel && logLevel <= this.MaxLevel;

    private sealed class Logger : Microsoft.Extensions.Logging.ILogger
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

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) =>
            httpLoggerProvider.IsEnabled(logLevel);

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            httpLoggerProvider.logger.Log(category, logLevel, eventId, state, exception, formatter);
        }
    }
}
