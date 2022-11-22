using System;
using System.Collections.Concurrent;
using System.Threading;

namespace DaJet.RabbitMQ
{
    public interface IDeliveryEventProcessor
    {
        void Process(DeliveryEvent @event);
        void Synchronize();
    }
    public abstract class DeliveryTracker
    {
        private long _shutdown = 0L;
        private string _reason = string.Empty;
        protected readonly ConcurrentDictionary<ulong, OutMessageInfo> _events = new ConcurrentDictionary<ulong, OutMessageInfo>(1, 1000);

        protected DeliveryTracker() { }
        public abstract void ConfigureDatabase();
        public abstract void RegisterEvent(DeliveryEvent @event);
        public abstract int ProcessEvents(IDeliveryEventProcessor processor);
        internal abstract void FlushEvents();
        internal void ClearEvents() { _events.Clear(); }
        
        internal void RegisterSelectEvent(ulong deliveryTag, OutMessageInfo message)
        {
            _ = _events.TryAdd(deliveryTag, message);
        }
        internal void RegisterPublishEvent(ulong deliveryTag)
        {
            OutMessageInfo info = _events[deliveryTag];

            info.EventType = DeliveryEventTypes.DBRMQ_PUBLISH;
            info.EventPublish = DateTime.UtcNow;
        }
        internal void SetAckStatus(ulong deliveryTag, bool multiple)
        {
            if (multiple)
            {
                SetMultipleStatus(deliveryTag, DeliveryEventTypes.DBRMQ_ACK);
            }
            else
            {
                SetSingleStatus(deliveryTag, DeliveryEventTypes.DBRMQ_ACK);
            }
        }
        internal void SetNackStatus(ulong deliveryTag, bool multiple)
        {
            if (multiple)
            {
                SetMultipleStatus(deliveryTag, DeliveryEventTypes.DBRMQ_NACK);
            }
            else
            {
                SetSingleStatus(deliveryTag, DeliveryEventTypes.DBRMQ_NACK);
            }
        }
        private void SetSingleStatus(ulong deliveryTag, DeliveryEventTypes eventType)
        {
            OutMessageInfo info = _events[deliveryTag];

            info.EventType = eventType;
            info.EventConfirm = DateTime.UtcNow;
        }
        private void SetMultipleStatus(ulong deliveryTag, DeliveryEventTypes eventType)
        {
            OutMessageInfo info;

            foreach (var item in _events)
            {
                if (item.Key <= deliveryTag)
                {
                    info = item.Value;

                    if (info.EventConfirm == DateTime.MinValue)
                    {
                        info.EventType = eventType;
                        info.EventConfirm = DateTime.UtcNow;
                    }
                }
            }
        }
        internal void SetReturnStatus(string messageId, string reason)
        {
            if (!Guid.TryParse(messageId, out Guid uuid))
            {
                return;
            }

            foreach (OutMessageInfo info in _events.Values)
            {
                if (info.MsgUid == uuid)
                {
                    info.Body = reason;
                    info.EventReturn = DateTime.UtcNow;
                    break;
                }
            }
        }
        internal void SetShutdownStatus(string reason)
        {
            Interlocked.Increment(ref _shutdown);

            _reason = reason;
        }
    }
}