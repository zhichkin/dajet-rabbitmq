namespace DaJet.RabbitMQ
{
    public sealed class RmqProducerOptions
    {
        public bool UseVectorService { get; set; } = false;
        public string VectorDatabase { get; set; } = string.Empty;
    }
}