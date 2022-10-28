using System;
using System.Collections.Concurrent;

namespace DaJet.RabbitMQ
{
    public interface IDeliveryEventProcessor
    {
        void Process(DeliveryEvent @event);
    }
    public abstract class DeliveryTracker : IDisposable
    {
        private readonly ConcurrentDictionary<ulong, DeliveryEvent> _tags = new ConcurrentDictionary<ulong, DeliveryEvent>();
        protected DeliveryTracker() { }
        public abstract void ConfigureDatabase();
        public abstract void RegisterEvent(DeliveryEvent @event);
        public abstract void ProcessEvents(IDeliveryEventProcessor processor);
        public void Dispose()
        {
            _tags.Clear();
        }
        public void Track(DeliveryEvent @event)
        {
            _ = _tags.TryAdd(@event.DeliveryTag, @event);
        }
        public void SetAckStatus(ulong deliveryTag, bool multiple)
        {
            if (multiple)
            {
                SetMultipleStatus(deliveryTag, PublishStatus.Ack);
            }
            else
            {
                SetSingleStatus(deliveryTag, PublishStatus.Ack);
            }
        }
        public void SetNackStatus(ulong deliveryTag, bool multiple)
        {
            if (multiple)
            {
                SetMultipleStatus(deliveryTag, PublishStatus.Nack);
            }
            else
            {
                SetSingleStatus(deliveryTag, PublishStatus.Nack);
            }
        }
        private void SetSingleStatus(ulong deliveryTag, PublishStatus status)
        {
            RegisterDeliveryStatus(_tags[deliveryTag], status);
        }
        private void SetMultipleStatus(ulong deliveryTag, PublishStatus status)
        {
            foreach (var item in _tags)
            {
                if (item.Key <= deliveryTag)
                {
                    RegisterDeliveryStatus(_tags[item.Key], status);
                }
            }
        }
        private void RegisterDeliveryStatus(DeliveryEvent @event, PublishStatus status)
        {
            string eventType = DeliveryEventType.UNDEFINED;

            if (status == PublishStatus.Ack)
            {
                eventType = DeliveryEventType.DBRMQ_ACK;
            }
            else if (status == PublishStatus.Nack)
            {
                eventType = DeliveryEventType.DBRMQ_NACK;
            }

            RegisterEvent(new DeliveryEvent()
            {
                DeliveryTag = @event.DeliveryTag,
                Source = @event.Source,
                MsgUid = @event.MsgUid,
                EventNode = @event.EventNode,
                EventType = eventType
            });
        }
        public void SetReturnedStatus(string appId, string messageId, string reason)
        {
            if (!Guid.TryParse(messageId, out Guid msgUid))
            {
                return;
            }

            RegisterEvent(new DeliveryEvent()
            {
                EventType = DeliveryEventType.DBRMQ_RETURN,
                Source = appId,
                MsgUid = msgUid,
                EventData = new ReturnEvent() { Reason = reason }
            });
        }
        public void SetShutdownStatus(string reason)
        {
            //if (!string.IsNullOrEmpty(reason))
            //{
            //    RegisterEvent(new DeliveryEvent()
            //    {
            //        EventType = DeliveryEventType.DBRMQ_SHUTDOWN,
            //        EventData = new ShutdownEvent() { Reason = reason }
            //    });
            //}
        }
    }
}