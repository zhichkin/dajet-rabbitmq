using System;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Unicode;

namespace DaJet.RabbitMQ
{
    public sealed class OutMessageInfo
    {
        public Guid MsgUid { get; set; } = Guid.Empty; // message uuid
        public string Type { get; set; } = string.Empty; // message type
        public string Body { get; set; } = string.Empty; // message entity key
        public string AppId { get; set; } = string.Empty; // message sender node
        public string Recipients { get; set; } = string.Empty; // recipients
        public string Vector { get; set; } = string.Empty; // vector clock value
        public string EventNode { get; set; } = string.Empty; // event source node
        public DeliveryEventTypes EventType { get; set; } = DeliveryEventTypes.UNDEFINED; // message delivery status
        public DateTime EventSelect { get; set; } = DateTime.MinValue;
        public DateTime EventPublish { get; set; } = DateTime.MinValue;
        public DateTime EventConfirm { get; set; } = DateTime.MinValue;
        public DateTime EventReturn { get; set; } = DateTime.MinValue;
    }
    public sealed class DeliveryEvent
    {
        [JsonIgnore] public bool Delivered { get; set; } = false;
        [JsonIgnore] public ulong DeliveryTag { get; set; } = ulong.MinValue;
        [JsonPropertyName("msguid")] public Guid MsgUid { get; set; } = Guid.Empty;
        [JsonPropertyName("source")] public string Source { get; set; } = string.Empty;
        [JsonPropertyName("node")] public string EventNode { get; set; } = string.Empty;
        [JsonPropertyName("time")] public DateTime EventTime { get; set; } = DateTime.UtcNow;
        [JsonPropertyName("type")] public string EventType { get; set; } = DeliveryEventType.UNDEFINED;
        [JsonPropertyName("data")] public object EventData { get; set; } = null;
        public string SerializeEventDataToJson()
        {
            if (EventData is null)
            {
                return string.Empty;
            }

            return JsonSerializer.Serialize(EventData, EventData.GetType(),
                new JsonSerializerOptions()
                {
                    WriteIndented = false,
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
                });
        }
    }
    public sealed class MessageData
    {
        [JsonPropertyName("target")] public string Target { get; set; } = string.Empty; // recipient[s]
        [JsonPropertyName("type")] public string Type { get; set; } = string.Empty; // message type
        [JsonPropertyName("body")] public string Body { get; set; } = string.Empty; // message body
        [JsonPropertyName("vector")] public string Vector { get; set; } = string.Empty; // message vector clock
        public string ToJson()
        {
            return JsonSerializer.Serialize(this, typeof(MessageData),
                new JsonSerializerOptions()
                {
                    WriteIndented = false,
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
                });
        }
    }
    public sealed class ReturnEvent
    {
        [JsonPropertyName("reason")] public string Reason { get; set; } = string.Empty;
        public string ToJson()
        {
            return JsonSerializer.Serialize(this, typeof(ReturnEvent),
                new JsonSerializerOptions()
                {
                    WriteIndented = false,
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
                });
        }
    }
    public sealed class ShutdownEvent
    {
        [JsonPropertyName("reason")] public string Reason { get; set; } = string.Empty;
    }
}