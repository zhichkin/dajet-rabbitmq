using System;
using System.Text.Json.Serialization;

namespace DaJet.RabbitMQ
{
    public sealed class TrackerEvent
    {
        [JsonIgnore] public ulong DeliveryTag { get; set; } = ulong.MinValue;
        [JsonPropertyName("source")] public string Source { get; set; } = string.Empty;
        [JsonPropertyName("msguid")] public string MsgUid { get; set; } = string.Empty;
        [JsonPropertyName("node")] public string EventNode { get; set; } = string.Empty;
        [JsonPropertyName("time")] public DateTime EventTime { get; set; } = DateTime.UtcNow;
        [JsonPropertyName("type")] public string EventType { get; set; } = TrackerEventType.UNDEFINED;
        [JsonPropertyName("data")] public object EventData { get; set; } = null;
    }
    public sealed class MessageData
    {
        [JsonPropertyName("target")] public string Target { get; set; } = string.Empty; // recipient[s]
        [JsonPropertyName("type")] public string Type { get; set; } = string.Empty; // message type
        [JsonPropertyName("body")] public string Body { get; set; } = string.Empty; // message body
        [JsonPropertyName("vector")] public string Vector { get; set; } = string.Empty; // message vector clock
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