using DaJet.Logging;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace DaJet.RabbitMQ
{
    public enum PublishStatus { New, Ack, Nack }
    internal sealed class PublishTracker
    {
        private long _nack = 0L;
        private long _returned = 0L;
        private long _shutdown = 0L;
        private string _reason = string.Empty;

        private readonly object _lock = new object();

        private readonly ConcurrentDictionary<ulong, PublishStatus> _tags = new ConcurrentDictionary<ulong, PublishStatus>(1, 1000);

        internal PublishTracker() { }

        internal void Track(ulong deliveryTag)
        {
            _ = _tags.TryAdd(deliveryTag, PublishStatus.New);
        }
        internal void Clear()
        {
            lock (_lock)
            {
                _tags.Clear();
            }
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

            Clear();

            _reason = reason;
        }
        internal void SetSingleStatus(ulong deliveryTag, PublishStatus status)
        {
            if (IsNacked || IsShutdown)
            {
                return;
            }

            if (status == PublishStatus.Ack)
            {
                lock (_lock)
                {
                    _ = _tags.TryRemove(deliveryTag, out _);
                }
            }
            else if (status == PublishStatus.Nack)
            {
                Interlocked.Increment(ref _nack); Clear();
            }
        }
        internal void SetMultipleStatus(ulong deliveryTag, PublishStatus status)
        {
            if (IsNacked || IsShutdown)
            {
                return;
            }

            if (status == PublishStatus.Ack)
            {
                lock (_lock)
                {
                    List<ulong> remove = new List<ulong>();

                    foreach (var item in _tags)
                    {
                        if (item.Key <= deliveryTag)
                        {
                            remove.Add(item.Key);
                        }
                    }

                    foreach (ulong key in remove)
                    {
                        _ = _tags.TryRemove(key, out _);
                    }
                }
            }
            else if (status == PublishStatus.Nack)
            {
                Interlocked.Increment(ref _nack); Clear();
            }
        }

        internal bool IsNacked
        {
            get
            {
                return (Interlocked.Read(ref _nack) > 0);
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
                if (string.IsNullOrWhiteSpace(_reason))
                {
                    _reason = "Some messages were nacked.";
                }
                return _reason;
            }
        }
        internal bool HasErrors()
        {
            if (IsShutdown || IsNacked)
            {
                return true;
            }

            if (_tags.Count > 0)
            {
                FileLogger.Log("[PublishTracker] Unexpected publish error.");
                return true;
            }

            return false;
        }
    }
}