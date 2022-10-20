using System;
using System.Text.Json.Serialization;

namespace DaJet.RabbitMQ
{
    public sealed class TrackerEvent
    {
        [JsonIgnore] public long RowId { get; set; } = 0L;
        [JsonIgnore] public ulong DeliveryTag { get; set; } = ulong.MinValue;
        [JsonPropertyName("node")] public string EventNode { get; set; } = string.Empty;
        [JsonPropertyName("time")] public DateTime EventTime { get; set; } = DateTime.UtcNow;
        [JsonPropertyName("type")] public string EventType { get; set; } = string.Empty;
        [JsonPropertyName("data")] public object EventData { get; set; } = null;
        [JsonPropertyName("source")] public string Source { get; set; } = string.Empty;
        [JsonPropertyName("msgid")] public string MessageId { get; set; } = string.Empty;
    }
    public sealed class MessageData
    {
        [JsonPropertyName("target")] public string Target { get; set; } = string.Empty; // recipient[s]
        [JsonPropertyName("type")] public string Type { get; set; } = string.Empty; // message type
        [JsonPropertyName("body")] public string Body { get; set; } = string.Empty; // message body
    }
    public sealed class ReturnEvent
    {
        [JsonPropertyName("reason")] public string Reason { get; set; } = string.Empty;
    }
    public sealed class ShutdownEvent
    {
        [JsonPropertyName("reason")] public string Reason { get; set; } = string.Empty;
    }
}