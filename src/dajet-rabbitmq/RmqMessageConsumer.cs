using DaJet.Data.Messaging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
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

        public string HostName { get; private set; } = "localhost";
        public int HostPort { get; private set; } = 5672;
        public string VirtualHost { get; private set; } = "/";
        public string UserName { get; private set; } = "guest";
        public string Password { get; private set; } = "guest";

        public RmqMessageConsumer(in string uri, in List<string> queues)
        {
            ParseRmqUri(in uri);

            foreach (string queue in queues)
            {
                _ = Consumers.TryAdd(queue, null);
            }
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
                    Task.Delay(TimeSpan.FromSeconds(10)).Wait(_token);
                    _logger("Consumer heartbeat.");
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

            try
            {
                IModel channel = Connection.CreateModel();
                
                channel.BasicQos(0, 1, false);

                consumer = new EventingBasicConsumer(channel);
                consumer.Received += ProcessMessage;

                consumerTag = channel.BasicConsume(queue, false, consumer);
            }
            catch
            {
                if (consumerTag != null)
                {
                    DisposeConsumer(consumerTag);
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

                if (consumer.Model != null)
                {
                    consumer.Model.Dispose();
                    consumer.Model = null;
                }

                _ = Consumers.TryUpdate(queueName, null, consumer);
            }

            //if (IsConsumerHealthy(consumer))
            //{
            //    consumer.Model.BasicCancel(consumer.ConsumerTags[0]);
            //}
        }

        private void ProcessMessage(object sender, BasicDeliverEventArgs args)
        {
            if (!(sender is EventingBasicConsumer consumer)) return;

            bool success = true;

            try
            {
                using (IMessageProducer producer = new MsMessageProducer(in _connectionString, _queue))
                {
                    IncomingMessageDataMapper message = ProduceMessage(in args);

                    producer.Insert(in message);

                    consumer.Model.BasicAck(args.DeliveryTag, false);
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

            message.Uuid = Guid.NewGuid();
            message.DateTimeStamp = DateTime.Now.AddYears(_yearOffset);
            message.MessageNumber = DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
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

            message.Uuid = Guid.NewGuid();
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
        private IncomingMessageDataMapper ProduceMessage12(in BasicDeliverEventArgs args)
        {
            V12.IncomingMessage message = IncomingMessageDataMapper.Create(_version) as V12.IncomingMessage;

            message.MessageNumber = DateTime.UtcNow.Ticks;
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
    }
}