using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;

namespace DaJet.Telegram.Bot
{
    // https://core.telegram.org/bots/api
    public interface IDaJetTelegramBot : IDisposable
    {
        void SendMessage(string chat, string message);
        Task SendMessageAsync(string chat, string message);
    }
    public sealed class DaJetTelegramBot : IDaJetTelegramBot
    {
        private const string API_BASE = "https://api.telegram.org";
        private const string BOT_CANNEL = "@dajet_bot";
        private const string BOT_TOKEN = "5386638934:AAF3Xo5zYcMBw2RdXrlF_YsbjRxRermz9Es";
        private HttpClient HttpClient { get; set; } = new HttpClient();
        public DaJetTelegramBot()
        {
            ConfigureHttpClient();
        }
        private void ConfigureHttpClient()
        {
            HttpClient.BaseAddress = new Uri(API_BASE);
        }
        public void Dispose()
        {
            HttpClient?.Dispose();
            HttpClient = null;
        }
        public void SendMessage(string chat, string message)
        {
            string url = $"/bot{BOT_TOKEN}/sendMessage";

            SendMessageRequest request = new SendMessageRequest()
            {
                Chat = (string.IsNullOrWhiteSpace(chat) ? BOT_CANNEL : chat),
                Text = message
            };

            Task<HttpResponseMessage> task = HttpClient.PostAsJsonAsync(url, request);

            if (!task.Wait(TimeSpan.FromSeconds(1)))
            {
                throw new TimeoutException();
            }
            else
            {
                HttpResponseMessage response = task.Result;

                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new Exception($"{(int)response.StatusCode}: {response.ReasonPhrase}");
                }
            }
        }
        public async Task SendMessageAsync(string chat, string message)
        {
            string url = $"/bot{BOT_TOKEN}/sendMessage";

            SendMessageRequest request = new SendMessageRequest()
            {
                Chat = (string.IsNullOrWhiteSpace(chat) ? BOT_CANNEL : chat),
                Text = message
            };

            HttpResponseMessage response = await HttpClient.PostAsJsonAsync(url, request);

            if (response.StatusCode != HttpStatusCode.OK)
            {
                throw new Exception($"{(int)response.StatusCode}: {response.ReasonPhrase}");
            }
        }
    }
}