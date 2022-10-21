using System;
using System.Collections.Generic;
using System.Net.Http;
#if (NETCORE30 || NETSTANDARD21)
using System.Net.Security;
#endif

namespace NLog.Targets.Http
{
    internal class HttpClientPool
    {
        private static HttpClientPool? _instance;
        private readonly object _lock = new object();
        private Dictionary<HttpClientParams, HttpClientReference> _clients = new Dictionary<HttpClientParams, HttpClientReference>();

        public static HttpClientPool Instance => _instance ??= new HttpClientPool();

        public IDisposable Aquire(HTTP2 owner, out HttpClient httpClient)
        {
            var clientParams = new HttpClientParams(owner);
            HttpClientReference httpClientReference;
            lock (_lock)
            {
                if (!_clients.TryGetValue(clientParams, out httpClientReference!))
                {
                    _clients[clientParams] = httpClientReference = new HttpClientReference(clientParams.Create());
                }
            }

            httpClient = httpClientReference.HttpClient;
            return httpClientReference;
        }

        private sealed class HttpClientReference : IDisposable
        {
            private int _refCount;

            public HttpClientReference(HttpClient httpClient)
            {
                _refCount = 1;
                HttpClient = httpClient;
            }

            public HttpClient HttpClient { get; }

            public void Dispose()
            {
                if (--_refCount == 0)
                {
                    HttpClient.Dispose();
                }
            }
        }
    }
}
