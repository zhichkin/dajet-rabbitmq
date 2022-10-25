namespace DaJet.RabbitMQ
{
    public static class TrackerEventType
    {
        public static readonly string UNDEFINED = "UNDEFINED";

        public static readonly string DBRMQ_SELECT = "DBRMQ_SELECT";
        public static readonly string DBRMQ_PUBLISH = "DBRMQ_PUBLISH";
        public static readonly string DBRMQ_ACK = "DBRMQ_ACK";
        public static readonly string DBRMQ_NACK = "DBRMQ_NACK";
        public static readonly string DBRMQ_RETURN = "DBRMQ_RETURN";
        public static readonly string DBRMQ_SHUTDOWN = "DBRMQ_SHUTDOWN";

        public static readonly string RMQDB_CONSUME = "RMQDB_CONSUME";
        public static readonly string RMQDB_INSERT = "RMQDB_INSERT";
        public static readonly string RMQDB_SHUTDOWN = "RMQDB_SHUTDOWN";
    }
}