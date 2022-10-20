using System.Collections.Generic;

namespace DaJet.RabbitMQ
{
    public sealed class RmqConsumerOptions
    {
        public int Heartbeat { get; set; } = 300; // seconds
        public List<string> Queues { get; set; } = new List<string>();
        public bool UseVectorService { get; set; } = false;
        public string VectorDatabase { get; set; } = string.Empty;
        public bool UseLog { get; set; } = false;
        public string LogDatabase { get; set; } = "rmq-consumer.db";
        public int LogRetention { get; set; } = 24 * 7; // one week in hours
        public string Node { get; set; } = string.Empty;
    }
}