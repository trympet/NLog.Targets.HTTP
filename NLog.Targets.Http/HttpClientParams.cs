using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
#if (NETCORE30 || NETSTANDARD21)
using System.Net.Security;
#endif

namespace NLog.Targets.Http
{
    internal readonly struct HttpClientParams : IEquatable<HttpClientParams>
    {
        public HttpClientParams(HTTP2 owner)
        {
            Url = owner.Url;
            Timeout = TimeSpan.FromMilliseconds(owner.ConnectTimeout);
            Accept = owner.Accept;
            var useProxy = UseProxy = !string.IsNullOrWhiteSpace(owner.ProxyUrl);
            ProxyUrl = useProxy ? owner.ProxyUrl : null;
            ProxyUser = useProxy ? owner.ProxyUser : null;
            ProxyPassword = useProxy ? owner.ProxyPassword : null;
            Authorization = owner.Authorization;
            IgnoreSslErrors = owner.IgnoreSslErrors;
        }

        public bool UseProxy { get; }

        public string ProxyUser { get; }

        public string ProxyUrl { get; }

        public string ProxyPassword { get; }

        public string Url { get; init; }

        public TimeSpan Timeout { get; init; }

        public string Accept { get; init; }

        public string Authorization { get; init; }

        public bool IgnoreSslErrors { get; init; }

        public HttpClient Create()
        {
#if (NETCORE30 || NET5_0_OR_GREATER || NETCOREAPP3_1)
            var handler = HTTP2.Handler?.Invoke() ?? new SocketsHttpHandler
            {
                UseProxy = UseProxy,
            };
#elif NETSTANDARD21
            var handler = new HttpClientHandler
            {
                UseProxy = UseProxy
            };
#endif

            var client = new HttpClient(handler)
            {
                BaseAddress = new Uri(Url),
                Timeout = Timeout,
            };

            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(Accept));

            //if (handler.UseProxy)
            //{
            //    var useDefaultCredentials = string.IsNullOrWhiteSpace(ProxyUser);
            //    handler.Proxy = new WebProxy(new Uri(ProxyUrl))
            //    { UseDefaultCredentials = useDefaultCredentials };
            //    if (!useDefaultCredentials)
            //    {
            //        var cred = ProxyUser.Split('\\');
            //        handler.Proxy.Credentials = cred.Length == 1
            //            ? new NetworkCredential { UserName = ProxyUser, Password = ProxyPassword }
            //            : new NetworkCredential
            //            { Domain = cred[0], UserName = cred[1], Password = ProxyPassword };
            //    }
            //}

            if (!string.IsNullOrWhiteSpace(Authorization))
            {
                client.DefaultRequestHeaders.Authorization = GetAuthorizationHeader();
            }
            if (IgnoreSslErrors)
            {
#if NETCOREAPP3_0_OR_GREATER
                //handler.SslOptions = new System.Net.Security.SslClientAuthenticationOptions { RemoteCertificateValidationCallback = (sender, certificate, chain, errors) => true };
#elif NETSTANDARD21
                handler.ServerCertificateCustomValidationCallback = (message, certificate, chain, errors) => true;
#endif
            }

            return client;
        }

        public override bool Equals(object obj)
        {
            return obj is HttpClientParams @params && Equals(@params);
        }

        public bool Equals(HttpClientParams other)
        {
            return UseProxy == other.UseProxy &&
                   !UseProxy ||
                    (ProxyUser == other.ProxyUser &&
                       ProxyUrl == other.ProxyUrl &&
                       ProxyPassword == other.ProxyPassword) &&
                   Url == other.Url &&
                   Timeout.Equals(other.Timeout) &&
                   Accept == other.Accept &&
                   Authorization == other.Authorization &&
                   IgnoreSslErrors == other.IgnoreSslErrors;
        }

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.Add(UseProxy);
            hash.Add(ProxyUser);
            hash.Add(ProxyUrl);
            hash.Add(ProxyPassword);
            hash.Add(Url);
            hash.Add(Timeout);
            hash.Add(Accept);
            hash.Add(Authorization);
            hash.Add(IgnoreSslErrors);
            return hash.ToHashCode();
        }

        private AuthenticationHeaderValue GetAuthorizationHeader()
        {
            var parts = Authorization.Split(' ');
            return parts.Length == 1
                ? new AuthenticationHeaderValue(Authorization)
                : new AuthenticationHeaderValue(parts[0], string.Join(" ", parts.Skip(1)));
        }

        public static bool operator ==(HttpClientParams left, HttpClientParams right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(HttpClientParams left, HttpClientParams right)
        {
            return !(left == right);
        }
    }
}
