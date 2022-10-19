using System;
using System.Text.Json.Serialization;

namespace DaJet.RabbitMQ
{
    public sealed class TrackerEvent
    {
        [JsonIgnore] public long RowId { get; set; } = 0L;
        [JsonIgnore] public ulong DeliveryTag { get; set; } = ulong.MinValue;
        public DateTime EventTime { get; set; } = DateTime.Now;
        public string EventType { get; set; } = string.Empty;
        public object EventData { get; set; } = null;
        public string Source { get; set; } = string.Empty;
        public string MessageId { get; set; } = string.Empty;
    }
    public sealed class MessageData
    {
        public string Target { get; set; } = string.Empty; // recipient[s]
        public string Type { get; set; } = string.Empty; // message type
        public string Body { get; set; } = string.Empty; // message body
    }
    public sealed class ReturnEvent
    {
        public string Reason { get; set; } = string.Empty;
    }
    public sealed class ShutdownEvent
    {
        public string Reason { get; set; } = string.Empty;
    }
}