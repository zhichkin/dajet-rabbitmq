using System;

namespace DaJet.RabbitMQ
{
    internal sealed class TrackerEvent
    {
        internal long RowId { get; set; } = 0L;
        internal ulong DeliveryTag { get; set; } = ulong.MinValue;
        internal DateTime EventTime { get; set; } = DateTime.Now;
        internal string EventType { get; set; } = string.Empty;
        internal string EventData { get; set; } = string.Empty;
        internal string Source { get; set; } = string.Empty;
        internal string Target { get; set; } = string.Empty;
        internal string MessageId { get; set; } = string.Empty;
        internal string MessageType { get; set; } = string.Empty;
        internal string MessageBody { get; set; } = string.Empty;
    }
}