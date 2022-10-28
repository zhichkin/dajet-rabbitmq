using DaJet.Data.Messaging;
using DaJet.Logging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using DaJet.Vector;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using OptionsFactory = Microsoft.Extensions.Options.Options;
using V1 = DaJet.Data.Messaging.V1;
using V10 = DaJet.Data.Messaging.V10;
using V11 = DaJet.Data.Messaging.V11;
using V12 = DaJet.Data.Messaging.V12;

namespace DaJet.RabbitMQ
{
    public sealed class RmqMessageConsumer : IDisposable
    {
        private const string DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR
            = "Интерфейс данных входящей очереди не поддерживается.";

        private IConnection Connection;
        private readonly ConcurrentDictionary<string, EventingBasicConsumer> Consumers = new ConcurrentDictionary<string, EventingBasicConsumer>();
        private DeliveryTracker _eventTracker;

        public string HostName { get; private set; } = "localhost";
        public int HostPort { get; private set; } = 5672;
        public string VirtualHost { get; private set; } = "/";
        public string UserName { get; private set; } = "guest";
        public string Password { get; private set; } = "guest";

        public RmqMessageConsumer(in string uri)
        {
            ParseRmqUri(in uri);
        }
        private void ParseRmqUri(in string amqpUri)
        {
            // amqp://guest:guest@localhost:5672/%2F

            Uri uri = new Uri(amqpUri);

            if (uri.Scheme != "amqp")
            {
                return;
            }

            HostName = uri.Host;
            HostPort = uri.Port;

            string[] userpass = uri.UserInfo.Split(':');
            if (userpass != null && userpass.Length == 2)
            {
                UserName = HttpUtility.UrlDecode(userpass[0], Encoding.UTF8);
                Password = HttpUtility.UrlDecode(userpass[1], Encoding.UTF8);
            }

            if (uri.Segments != null && uri.Segments.Length > 1)
            {
                if (uri.Segments.Length > 1)
                {
                    VirtualHost = HttpUtility.UrlDecode(uri.Segments[1].TrimEnd('/'), Encoding.UTF8);
                }
            }
        }

        private Action<string> _logger;

        private int _version;
        private int _yearOffset = 0;
        private string _metadataName;
        private ApplicationObject _queue;
        private string _connectionString;
        private DatabaseProvider _provider;

        private CancellationToken _token;
        private int _consumed = 0;

        private ConsumerLogger _consumerLogger;
        private IDaJetVectorService _vectorService;

        public IOptions<RmqConsumerOptions> Options { get; private set; }
        public void Configure(IOptions<RmqConsumerOptions> options)
        {
            Options = options;

            _eventTracker = new MsDeliveryTracker("Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True;Encrypt=False;");
            _eventTracker.ConfigureDatabase();

            if (Options.Value.UseVectorService && !string.IsNullOrWhiteSpace(Options.Value.VectorDatabase))
            {
                VectorServiceOptions settings = new VectorServiceOptions()
                {
                    ConnectionString = Options.Value.VectorDatabase
                };
                IOptions<VectorServiceOptions> vectorOptions = OptionsFactory.Create(settings);

                _vectorService = new VectorService(vectorOptions);
            }

            if (Options.Value.UseLog)
            {
                _consumerLogger = new ConsumerLogger(Options.Value.LogDatabase, Options.Value.LogRetention);
            }
        }

        public void Initialize(DatabaseProvider provider, string connectionString, string metadataName)
        {
            _provider = provider;
            _connectionString = connectionString;
            _metadataName = metadataName;
        }
        private void InitializeMetadata()
        {
            if (!new MetadataService()
                .UseDatabaseProvider(_provider)
                .UseConnectionString(_connectionString)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                throw new Exception(error);
            }

            _yearOffset = infoBase.YearOffset;

            _queue = infoBase.GetApplicationObjectByName(_metadataName);
            if (_queue == null)
            {
                throw new Exception($"Объект метаданных \"{_metadataName}\" не найден.");
            }

            _version = GetDataContractVersion(in _queue);
            if (_version < 1)
            {
                throw new Exception(DATABASE_INTERFACE_IS_NOT_SUPPORTED_ERROR);
            }
        }
        private int GetDataContractVersion(in ApplicationObject queue)
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();

            return validator.GetIncomingInterfaceVersion(in queue);
        }

        public void Consume(CancellationToken token, Action<string> logger)
        {
            _token = token;
            _logger = logger;

            while (!_token.IsCancellationRequested)
            {
                try
                {
                    InitializeMetadata();
                    InitializeOrResetConnection();
                    InitializeOrResetConsumers();

                    int heartbeat = 300; // seconds

                    if (Options != null && Options.Value != null)
                    {
                        heartbeat = Options.Value.Heartbeat;
                    }

                    Task.Delay(TimeSpan.FromSeconds(heartbeat)).Wait(_token);

                    int consumed = Interlocked.Exchange(ref _consumed, 0);

                    _logger($"Consumed {consumed} messages.");

                    if (Options.Value.UseLog)
                    {
                        _consumerLogger?.ClearLog();
                    }
                }
                catch (Exception error)
                {
                    _logger(ExceptionHelper.GetErrorText(error));
                }
            }
        }
        public void Dispose()
        {
            if (Connection != null)
            {
                if (Connection.IsOpen)
                {
                    Connection.Close();
                }
                Connection.Dispose();
                Connection = null;
            }

            foreach (var consumer in Consumers)
            {
                DisposeConsumer(consumer.Key);
            }
        }

        private IConnection CreateConnection()
        {
            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = HostName,
                Port = HostPort,
                VirtualHost = VirtualHost,
                UserName = UserName,
                Password = Password,
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
            };
            return factory.CreateConnection();
        }
        private void InitializeOrResetConnection()
        {
            if (Connection == null)
            {
                Connection = CreateConnection();
            }
            else if (!Connection.IsOpen)
            {
                Dispose();
                Connection = CreateConnection();
            }
        }

        private void InitializeOrResetConsumers()
        {
            UpdateConsumers();

            foreach (var consumer in Consumers)
            {
                if (consumer.Value == null)
                {
                    StartConsumerTask(consumer.Key);
                }
                else if (!IsConsumerHealthy(consumer.Value))
                {
                    ResetConsumerTask(consumer.Key);
                }
            }
        }
        private void UpdateConsumers()
        {
            List<string> settings = Options.Value.Queues;

            List<string> consumers = new List<string>(Consumers.Count);
            foreach (var item in Consumers)
            {
                consumers.Add(item.Key);
            }

            ListMergeHelper.Compare(consumers, settings, out List<string> delete, out List<string> insert);

            if (delete.Count == 0 && insert.Count == 0)
            {
                _logger("Queue consumer list is not changed.");
                return;
            }

            foreach (string queueName in insert)
            {
                if (Consumers.TryAdd(queueName, null))
                {
                    _logger($"Queue {queueName} consumer is added.");
                }
            }

            foreach (string queueName in delete)
            {
                DisposeConsumer(queueName);

                if (Consumers.TryRemove(queueName, out EventingBasicConsumer consumer))
                {
                    _logger($"Queue {queueName} consumer is removed.");
                }
            }
        }
        private void StartConsumerTask(string queueName)
        {
            _ = Task.Factory.StartNew(
                StartNewConsumer,
                queueName,
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }
        private void StartNewConsumer(object queueName)
        {
            if (!(queueName is string queue)) return;

            string consumerTag = null;
            EventingBasicConsumer consumer = null;
            IModel channel = Connection.CreateModel();

            try
            {
                channel.BasicQos(0, 1, false);

                consumer = new EventingBasicConsumer(channel);
                consumer.Received += ProcessMessage;

                consumerTag = channel.BasicConsume(queue, false, consumer);
            }
            catch (Exception error)
            {
                if (consumerTag != null)
                {
                    DisposeConsumer(consumerTag);
                }
                else
                {
                    DisposeConsumer(consumer);
                }

                if (error.Message.Contains(queue))
                {
                    _logger(error.Message); // queue is not found by RabbitMQ
                }

                throw; // Завершаем поток (задачу) с ошибкой
            }

            _ = Consumers.TryUpdate(queue, consumer, null);
        }
        private bool IsConsumerHealthy(EventingBasicConsumer consumer)
        {
            return (consumer != null
                && consumer.Model != null
                && consumer.Model.IsOpen
                && consumer.IsRunning);
        }
        private void ResetConsumerTask(string queueName)
        {
            DisposeConsumer(queueName);
            StartConsumerTask(queueName);
        }
        private void DisposeConsumer(string queueName)
        {
            if (!Consumers.TryGetValue(queueName, out EventingBasicConsumer consumer))
            {
                return;
            }

            if (consumer != null)
            {
                consumer.Received -= ProcessMessage;
                consumer.Model?.Dispose();
                consumer.Model = null;

                _ = Consumers.TryUpdate(queueName, null, consumer);
            }

            //if (IsConsumerHealthy(consumer))
            //{
            //    consumer.Model.BasicCancel(consumer.ConsumerTags[0]);
            //}
        }
        private void DisposeConsumer(EventingBasicConsumer consumer)
        {
            if (consumer != null)
            {
                consumer.Received -= ProcessMessage;
                consumer.Model?.Dispose();
                consumer.Model = null;
                consumer = null;
            }
        }

        private void ProcessMessage(object sender, BasicDeliverEventArgs args)
        {
            if (!(sender is EventingBasicConsumer consumer)) return;

            if (Options.Value.UseLog)
            {
                LogMessageDelivery(in args);
            }

            if (Options.Value.UseDeliveryTracking)
            {
                TrackConsumeEvent(args.BasicProperties, args.Body);
            }

            bool success = true;

            try
            {
                using (IMessageProducer producer = GetMessageProducer())
                {
                    IncomingMessageDataMapper message = ProduceMessage(in args);

                    producer.Insert(in message);

                    if (Options.Value.UseDeliveryTracking)
                    {
                        TrackInsertEvent(args.BasicProperties, args.Body);
                    }

                    if (Options.Value.UseVectorService)
                    {
                        ValidateVector(in args);
                    }

                    consumer.Model.BasicAck(args.DeliveryTag, false);

                    Interlocked.Increment(ref _consumed);
                }
            }
            catch (Exception error)
            {
                success = false;
                _logger(ExceptionHelper.GetErrorText(error));
            }

            if (!success)
            {
                NackMessage(in consumer, in args);
            }
        }
        private IMessageProducer GetMessageProducer()
        {
            if (_provider == DatabaseProvider.SQLServer)
            {
                return new MsMessageProducer(in _connectionString, _queue);
            }
            else
            {
                return new PgMessageProducer(in _connectionString, _queue);
            }
        }
        private void NackMessage(in EventingBasicConsumer consumer, in BasicDeliverEventArgs args)
        {
            if (!IsConsumerHealthy(consumer)) return;

            Task.Delay(TimeSpan.FromSeconds(10)).Wait(_token);

            try
            {
                consumer.Model.BasicNack(args.DeliveryTag, false, true);
            }
            catch (Exception error)
            {
                _logger(ExceptionHelper.GetErrorText(error));
            }
        }
        private IncomingMessageDataMapper ProduceMessage(in BasicDeliverEventArgs args)
        {
            if (_version == 1)
            {
                return ProduceMessage1(in args);
            }
            else if (_version == 10)
            {
                return ProduceMessage10(in args);
            }
            else if (_version == 11)
            {
                return ProduceMessage11(in args);
            }
            else if (_version == 12)
            {
                return ProduceMessage12(in args);
            }
            else
            {
                return null;
            }
        }
        private IncomingMessageDataMapper ProduceMessage1(in BasicDeliverEventArgs args)
        {
            V1.IncomingMessage message = IncomingMessageDataMapper.Create(_version) as V1.IncomingMessage;

            message.DateTimeStamp = DateTime.Now.AddYears(_yearOffset);
            message.MessageNumber = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
            message.ErrorCount = 0;
            message.ErrorDescription = String.Empty;
            message.Headers = GetMessageHeaders(in args);
            message.Sender = (args.BasicProperties.AppId ?? string.Empty);
            message.MessageType = (args.BasicProperties.Type ?? string.Empty);
            message.MessageBody = (args.Body.Length == 0 ? string.Empty : Encoding.UTF8.GetString(args.Body.Span));

            return message;
        }
        private IncomingMessageDataMapper ProduceMessage10(in BasicDeliverEventArgs args)
        {
            V10.IncomingMessage message = IncomingMessageDataMapper.Create(_version) as V10.IncomingMessage;

            Guid messageUid = Guid.Empty;

            if (!string.IsNullOrEmpty(args.BasicProperties.MessageId) &&
                !Guid.TryParse(args.BasicProperties.MessageId, out messageUid))
            {
                messageUid = Guid.NewGuid();
            }

            message.Uuid = messageUid;
            message.MessageNumber = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
            message.DateTimeStamp = DateTime.Now.AddYears(_yearOffset);
            message.ErrorCount = 0;
            message.ErrorDescription = String.Empty;
            message.Sender = (args.BasicProperties.AppId ?? string.Empty);
            message.MessageType = (args.BasicProperties.Type ?? string.Empty);
            message.MessageBody = (args.Body.Length == 0 ? string.Empty : Encoding.UTF8.GetString(args.Body.Span));

            if (args.BasicProperties.Headers != null)
            {
                if (args.BasicProperties.Headers.TryGetValue("OperationType", out object value))
                {
                    if (value is byte[] operationType)
                    {
                        message.OperationType = Encoding.UTF8.GetString(operationType);
                    }
                }
            }

            return message;
        }
        private IncomingMessageDataMapper ProduceMessage11(in BasicDeliverEventArgs args)
        {
            V11.IncomingMessage message = IncomingMessageDataMapper.Create(_version) as V11.IncomingMessage;

            Guid messageUid = Guid.Empty;

            if (!string.IsNullOrEmpty(args.BasicProperties.MessageId) &&
                !Guid.TryParse(args.BasicProperties.MessageId, out messageUid))
            {
                messageUid = Guid.NewGuid();
            }

            message.Uuid = messageUid;
            message.MessageNumber = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
            message.DateTimeStamp = DateTime.Now.AddYears(_yearOffset);
            message.ErrorCount = 0;
            message.ErrorDescription = String.Empty;
            message.Headers = GetMessageHeaders(in args);
            message.Sender = (args.BasicProperties.AppId ?? string.Empty);
            message.MessageType = (args.BasicProperties.Type ?? string.Empty);
            message.MessageBody = (args.Body.Length == 0 ? string.Empty : Encoding.UTF8.GetString(args.Body.Span));

            if (args.BasicProperties.Headers != null)
            {
                if (args.BasicProperties.Headers.TryGetValue("OperationType", out object value))
                {
                    if (value is byte[] operationType)
                    {
                        message.OperationType = Encoding.UTF8.GetString(operationType);
                    }
                }
            }

            return message;
        }
        private IncomingMessageDataMapper ProduceMessage12(in BasicDeliverEventArgs args)
        {
            V12.IncomingMessage message = IncomingMessageDataMapper.Create(_version) as V12.IncomingMessage;

            Guid messageUid = Guid.Empty;

            if (!string.IsNullOrEmpty(args.BasicProperties.MessageId) &&
                !Guid.TryParse(args.BasicProperties.MessageId, out messageUid))
            {
                messageUid = Guid.NewGuid();
            }

            message.Uuid = messageUid;
            message.MessageNumber = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
            message.DateTimeStamp = DateTime.Now.AddYears(_yearOffset);
            message.ErrorCount = 0;
            message.ErrorDescription = String.Empty;
            message.Headers = GetMessageHeaders(in args);
            message.Sender = (args.BasicProperties.AppId ?? string.Empty);
            message.MessageType = (args.BasicProperties.Type ?? string.Empty);
            message.MessageBody = (args.Body.Length == 0 ? string.Empty : Encoding.UTF8.GetString(args.Body.Span));

            return message;
        }
        private string GetMessageHeaders(in BasicDeliverEventArgs args)
        {
            if (args.BasicProperties.Headers == null || args.BasicProperties.Headers.Count == 0)
            {
                return string.Empty;
            }

            Dictionary<string, string> headers = new Dictionary<string, string>();

            foreach (var header in args.BasicProperties.Headers)
            {
                if (header.Value is byte[] value)
                {
                    try
                    {
                        headers.Add(header.Key, Encoding.UTF8.GetString(value));
                    }
                    catch
                    {
                        headers.Add(header.Key, string.Empty);
                    }
                }
            }

            if (headers.Count == 0)
            {
                return string.Empty;
            }

            return JsonSerializer.Serialize(headers);
        }
        private string GetHeaderVector(in IBasicProperties headers)
        {
            if (headers != null && headers.Headers != null)
            {
                if (headers.Headers.TryGetValue("vector", out object value))
                {
                    if (value is byte[] vector1)
                    {
                        return Encoding.UTF8.GetString(vector1);
                    }
                    else if (value is string vector2)
                    {
                        return vector2;
                    }
                }
            }
            return string.Empty;
        }

        private void ValidateVector(in BasicDeliverEventArgs args)
        {
            if (args == null || args.BasicProperties == null)
            {
                return;
            }

            try
            {
                TryValidateVector(in args);
            }
            catch (Exception error)
            {
                _logger(ExceptionHelper.GetErrorText(error));
            }
        }
        private void TryValidateVector(in BasicDeliverEventArgs args)
        {
            IBasicProperties headers = args.BasicProperties;

            string value = GetHeaderVector(in headers);
            if (string.IsNullOrEmpty(value)) { return; }

            if (!long.TryParse(value, out long vector) || vector <= 0L)
            {
                return;
            }

            string node = headers.AppId;
            if (string.IsNullOrEmpty(node)) { return; }

            string type = headers.Type;
            if (string.IsNullOrEmpty(type)) { return; }

            string key = MessageJsonParser.ExtractEntityKey(type, args.Body);
            if (string.IsNullOrEmpty(key)) { return; }

            _ = _vectorService?.ValidateVector(node, type, key, vector);
        }

        private void LogMessageDelivery(in BasicDeliverEventArgs args)
        {
            if (args == null || args.BasicProperties == null)
            {
                return;
            }

            try
            {
                TryLogMessageDelivery(in args);
            }
            catch (Exception error)
            {
                _logger(ExceptionHelper.GetErrorText(error));
            }
        }
        private void TryLogMessageDelivery(in BasicDeliverEventArgs args)
        {
            IBasicProperties headers = args.BasicProperties;

            string node = headers.AppId;
            string type = headers.Type;

            if (string.IsNullOrEmpty(type))
            {
                return;
            }

            string key = MessageJsonParser.ExtractEntityKey(type, args.Body);

            if (string.IsNullOrEmpty(key))
            {
                return;
            }

            _consumerLogger?.Log(node, type, key);
        }

        private void TrackConsumeEvent(in IBasicProperties headers, in ReadOnlyMemory<byte> message)
        {
            try
            {
                TryTrackConsumeEvent(in headers, message);
            }
            catch (Exception error)
            {
                FileLogger.Log(ExceptionHelper.GetErrorText(error));
            }
        }
        private void TryTrackConsumeEvent(in IBasicProperties headers, in ReadOnlyMemory<byte> message)
        {
            if (!Guid.TryParse(headers.MessageId, out Guid msgUid))
            {
                return;
            }

            DeliveryEvent @event = new DeliveryEvent()
            {
                Source = headers.AppId ?? string.Empty,
                MsgUid = msgUid,
                EventNode = Options.Value.ThisNode,
                EventType = DeliveryEventType.RMQDB_CONSUME,
                EventData = new MessageData()
                {
                    Target = Options.Value.ThisNode,
                    Type = headers.Type ?? string.Empty,
                    Body = MessageJsonParser.ExtractEntityKey(headers.Type ?? string.Empty, message),
                    Vector = GetHeaderVector(in headers)
                }
            };

            _eventTracker.RegisterEvent(@event);
        }
        private void TrackInsertEvent(in IBasicProperties headers, in ReadOnlyMemory<byte> message)
        {
            try
            {
                TryTrackInsertEvent(in headers, message);
            }
            catch (Exception error)
            {
                FileLogger.Log(ExceptionHelper.GetErrorText(error));
            }
        }
        private void TryTrackInsertEvent(in IBasicProperties headers, in ReadOnlyMemory<byte> message)
        {
            if (!Guid.TryParse(headers.MessageId, out Guid msgUid))
            {
                return;
            }

            DeliveryEvent @event = new DeliveryEvent()
            {
                Source = headers.AppId ?? string.Empty,
                MsgUid = msgUid,
                EventNode = Options.Value.ThisNode,
                EventType = DeliveryEventType.RMQDB_INSERT
            };

            _eventTracker.RegisterEvent(@event);
        }
    }
}