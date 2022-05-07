using System.Collections.Generic;

namespace DaJet.RabbitMQ
{
    public sealed class RmqConsumerOptions
    {
        public int Heartbeat { get; set; } = 300; // seconds
        public List<string> Queues { get; set; } = new List<string>();
        public bool UseVectorService { get; set; } = false;
        public string VectorDatabase { get; set; } = string.Empty;
    }
}