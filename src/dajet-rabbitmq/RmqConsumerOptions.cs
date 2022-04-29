using System;
using System.Collections.Generic;
using System.Text;

namespace DaJet.RabbitMQ
{
    public sealed class RmqConsumerOptions
    {
        public int Heartbeat { get; set; } = 300; // seconds
        public List<string> Queues { get; set; } = new List<string>();
    }
}