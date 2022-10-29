using System.Collections.Concurrent;
using System.Threading;

namespace DaJet.RabbitMQ
{
    public enum PublishStatus { New, Ack, Nack }
    internal sealed class PublishTracker
    {
        private long _returned = 0L;
        private long _shutdown = 0L;
        private string _reason = string.Empty;

        private readonly ConcurrentDictionary<ulong, PublishStatus> _tags = new ConcurrentDictionary<ulong, PublishStatus>();

        internal PublishTracker() { }

        internal void Track(ulong deliveryTag)
        {
            _ = _tags.TryAdd(deliveryTag, PublishStatus.New);
        }
        internal void Clear()
        {
            _tags.Clear();
        }

        internal void SetAckStatus(ulong deliveryTag, bool multiple)
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
        internal void SetNackStatus(ulong deliveryTag, bool multiple)
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
        internal void SetReturnedStatus(string reason)
        {
            Interlocked.Increment(ref _returned);

            _reason = reason;
        }
        internal void SetShutdownStatus(string reason)
        {
            Interlocked.Increment(ref _shutdown);

            _reason = reason;
        }
        internal void SetSingleStatus(ulong deliveryTag, PublishStatus status)
        {
            _tags[deliveryTag] = status;
        }
        internal void SetMultipleStatus(ulong deliveryTag, PublishStatus status)
        {
            foreach (var item in _tags)
            {
                if (item.Key <= deliveryTag)
                {
                    _tags[item.Key] = status;
                }
            }
        }

        internal bool IsReturned
        {
            get
            {
                return (Interlocked.Read(ref _returned) > 0);
            }
        }
        internal bool IsShutdown
        {
            get
            {
                return (Interlocked.Read(ref _shutdown) > 0);
            }
        }
        internal string ErrorReason
        {
            get
            {
                if (string.IsNullOrEmpty(_reason))
                {
                    _reason = "Some messages were nacked.";
                }
                return _reason;
            }
        }
        internal bool HasErrors()
        {
            if (IsShutdown) // IsReturned
            {
                return true;
            }

            foreach (var item in _tags)
            {
                if (item.Value != PublishStatus.Ack)
                {
                    return true;
                }
            }

            return false;
        }
    }
}