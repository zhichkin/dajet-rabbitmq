﻿using DaJet.Metadata;

namespace DaJet.RabbitMQ
{
    public sealed class RmqProducerOptions
    {
        public bool UseVectorService { get; set; } = false;
        public string VectorDatabase { get; set; } = string.Empty;
        public string ErrorLogDatabase { get; set; } = string.Empty;
        public int ErrorLogRetention { get; set; } = 24 * 7; // one week in hours
        public int MessagesPerTransaction { get; set; } = 1000;
        public string ThisNode { get; set; } = string.Empty;
        public bool UseDeliveryTracking { get; set; } = false;
        public string ConnectionString { get; set; } = string.Empty;
        public DatabaseProvider Provider { get; set; } = DatabaseProvider.SQLServer;
    }
}