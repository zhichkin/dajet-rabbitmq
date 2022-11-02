using DaJet.Data.Messaging;
using DaJet.Json;
using DaJet.Logging;
using DaJet.Metadata;
using DaJet.Vector;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using System.Web;
using OptionsFactory = Microsoft.Extensions.Options.Options;
using V1 = DaJet.Data.Messaging.V1;
using V10 = DaJet.Data.Messaging.V10;
using V11 = DaJet.Data.Messaging.V11;
using V12 = DaJet.Data.Messaging.V12;

namespace DaJet.RabbitMQ
{
    public sealed class RmqMessageProducer : IDeliveryEventProcessor, IDisposable
    {
        private IConnection Connection;
        private IModel Channel;
        private IBasicProperties Properties;
        private bool ConnectionIsBlocked = false;

        #region "PRIVATE FIELDS"

        private byte[] _buffer; // message body buffer
        private PublishTracker _tracker; // publisher confirms tracker
        private ExchangeRoles _exchangeRole = ExchangeRoles.None;
        private DeliveryTracker _eventTracker; // message delivery tracking service
        private bool _useDeliveryTracking = false;
        private readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions()
        {
            WriteIndented = false,
            Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
        };

        #endregion

        #region "RABBIT MQ SETTINGS"

        public string HostName { get; private set; } = "localhost";
        public int HostPort { get; private set; } = 5672;
        public string VirtualHost { get; private set; } = "/";
        public string UserName { get; private set; } = "guest";
        public string Password { get; private set; } = "guest";
        public string ExchangeName { get; private set; } = string.Empty; // if empty RoutingKey is a queue name to send directly
        public string RoutingKey { get; private set; } = string.Empty; // if exchange name is not empty this is routing key value

        #endregion

        public RmqMessageProducer(string uri, string routingKey)
        {
            ParseRmqUri(uri);
            RoutingKey = routingKey;
        }

        private IDaJetVectorService _vectorService;
        public IOptions<RmqProducerOptions> Options { get; private set; }
        private DeliveryTracker CreateDeliveryTracker()
        {
            if (Options.Value.Provider == DatabaseProvider.SQLServer)
            {
                return new MsDeliveryTracker(Options.Value.ConnectionString);
            }

            // TODO: PostgreSql provider
            return null;
        }
        public void Configure(IOptions<RmqProducerOptions> options)
        {
            Options = options;

            _useDeliveryTracking = Options.Value.UseDeliveryTracking;

            if (_useDeliveryTracking)
            {
                _eventTracker = CreateDeliveryTracker();
            }

            if (Options.Value.UseVectorService && !string.IsNullOrWhiteSpace(Options.Value.VectorDatabase))
            {
                VectorServiceOptions settings = new VectorServiceOptions()
                {
                    ConnectionString = Options.Value.VectorDatabase
                };
                IOptions<VectorServiceOptions> vectorOptions = OptionsFactory.Create(settings);

                _vectorService = new VectorService(vectorOptions);
            }
        }
        
        #region "RABBITMQ CONNECTION SETUP"

        public void Initialize()
        {
            Connection = CreateConnection();
            Channel = CreateChannel(Connection);
            Properties = CreateMessageProperties(Channel);
        }
        public void Initialize(ExchangeRoles role)
        {
            Initialize();
            _exchangeRole = role;
        }
        private void ParseRmqUri(string amqpUri)
        {
            // amqp://guest:guest@localhost:5672/%2F/РИБ.ERP

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

            if (uri.Segments != null && uri.Segments.Length == 3)
            {
                if (uri.Segments.Length > 1)
                {
                    VirtualHost = HttpUtility.UrlDecode(uri.Segments[1].TrimEnd('/'), Encoding.UTF8);
                }

                if (uri.Segments.Length == 3)
                {
                    ExchangeName = HttpUtility.UrlDecode(uri.Segments[2].TrimEnd('/'), Encoding.UTF8);
                }
            }
        }
        private IConnection CreateConnection()
        {
            //if (Connection != null && Connection.IsOpen) return;
            //if (Connection != null) Connection.Dispose();

            //if (Connection == null)
            //{
            //    Connection = CreateConnection();
            //}
            //else if (!Connection.IsOpen)
            //{
            //    Connection.Dispose();
            //    Connection = CreateConnection();
            //}

            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = HostName,
                Port = HostPort,
                VirtualHost = VirtualHost,
                UserName = UserName,
                Password = Password
            };
            IConnection connection = factory.CreateConnection();
            connection.ConnectionBlocked += HandleConnectionBlocked;
            connection.ConnectionUnblocked += HandleConnectionUnblocked;
            return connection;
        }
        private void HandleConnectionBlocked(object sender, ConnectionBlockedEventArgs args)
        {
            ConnectionIsBlocked = true;
        }
        private void HandleConnectionUnblocked(object sender, EventArgs args)
        {
            ConnectionIsBlocked = false;
        }
        private IModel CreateChannel(IConnection connection)
        {
            //if (Channel == null)
            //{
            //    Channel = Connection.CreateModel();
            //    Channel.ConfirmSelect();
            //    InitializeBasicProperties();
            //}
            //else if (Channel.IsClosed)
            //{
            //    Channel.Dispose();
            //    Channel = Connection.CreateModel();
            //    Channel.ConfirmSelect();
            //    InitializeBasicProperties();
            //}

            IModel channel = connection.CreateModel();
            channel.ConfirmSelect();
            channel.BasicAcks += BasicAcksHandler;
            channel.BasicNacks += BasicNacksHandler;
            channel.BasicReturn += BasicReturnHandler;
            channel.ModelShutdown += ModelShutdownHandler;
            return channel;
        }
        private IBasicProperties CreateMessageProperties(IModel channel)
        {
            IBasicProperties properties = channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent
            properties.ContentEncoding = "UTF-8";
            return properties;
        }
        public void Dispose()
        {
            if (Channel != null)
            {
                Channel.Dispose();
                Channel = null;
            }

            if (Connection != null)
            {
                Connection.Dispose();
                Connection = null;
            }

            if (_buffer != null)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
            }

            if (_tracker != null)
            {
                _tracker.Clear();
                _tracker = null;
            }

            if (_eventTracker != null)
            {
                _eventTracker.ClearEvents();
                _eventTracker = null;
            }
        }

        #endregion

        public int Publish(IDaJetJsonSerializer serializer, int pageSize, int pageNumber)
        {
            int messagesSent = 0;

            foreach (ReadOnlyMemory<byte> messageBody in serializer.Serialize(pageSize, pageNumber))
            {
                if (ConnectionIsBlocked)
                {
                    throw new Exception("Connection is blocked");
                }

                Properties.MessageId = Guid.NewGuid().ToString();

                Channel.BasicPublish(ExchangeName, RoutingKey, Properties, messageBody);

                messagesSent++;
            }

            if (messagesSent > 0)
            {
                if (!Channel.WaitForConfirms())
                {
                    throw new Exception("WaitForConfirms error");
                }
            }

            return messagesSent;
        }

        #region "TEST METHODS"

        public void Publish(OutgoingMessageDataMapper message)
        {
            ConfigureMessageProperties(in message, Properties, null);

            ReadOnlyMemory<byte> messageBody = GetMessageBody(in message);

            if (Options.Value.UseVectorService)
            {
                ValidateVector(Properties, messageBody);
            }

            if (_tracker == null)
            {
                _tracker = new PublishTracker();
            }
            _tracker.Track(Channel.NextPublishSeqNo);

            if (string.IsNullOrWhiteSpace(RoutingKey))
            {
                Channel.BasicPublish(ExchangeName, message.MessageType, true, Properties, messageBody);
            }
            else
            {
                Channel.BasicPublish(ExchangeName, RoutingKey, true, Properties, messageBody);
            }

            //if (!Channel.WaitForConfirms())
            //{
            //    throw new Exception("WaitForConfirms error");
            //}

            //if (tracker.HasErrors())
            //{
            //    throw new Exception(_tracker.ErrorReason);
            //}
        }
        public void Confirm()
        {
            if (!Channel.WaitForConfirms())
            {
                throw new Exception("WaitForConfirms error");
            }

            if (_tracker.HasErrors())
            {
                throw new Exception(_tracker.ErrorReason);
            }

            _tracker.Clear();
            _tracker = null;
        }

        #endregion

        public int Publish(IMessageConsumer consumer)
        {
            int produced = 0;

            do
            {
                consumer.TxBegin();

                _tracker = new PublishTracker();

                foreach (OutgoingMessageDataMapper message in consumer.Select(Options.Value.MessagesPerTransaction))
                {
                    if (ConnectionIsBlocked)
                    {
                        throw new Exception("Connection is blocked");
                    }

                    OutMessageInfo deliveryInfo = null;
                    if (_useDeliveryTracking)
                    {
                        deliveryInfo = new OutMessageInfo() { EventNode = Options.Value.ThisNode };
                    }
                    ConfigureMessageProperties(in message, Properties, in deliveryInfo);

                    ReadOnlyMemory<byte> messageBody = GetMessageBody(in message);

                    if (Options.Value.UseVectorService)
                    {
                        ValidateVector(Properties, messageBody);
                    }

                    ulong deliveryTag = Channel.NextPublishSeqNo;

                    _tracker.Track(deliveryTag);

                    if (_useDeliveryTracking)
                    {
                        deliveryInfo.Body = MessageJsonParser.ExtractEntityKey(deliveryInfo.Type, messageBody);
                        _eventTracker?.RegisterSelectEvent(deliveryTag, deliveryInfo);
                    }

                    if (string.IsNullOrWhiteSpace(RoutingKey))
                    {
                        Channel.BasicPublish(ExchangeName, message.MessageType, true, Properties, messageBody);
                    }
                    else
                    {
                        Channel.BasicPublish(ExchangeName, RoutingKey, true, Properties, messageBody);
                    }

                    if (_useDeliveryTracking)
                    {
                        _eventTracker?.RegisterPublishEvent(deliveryTag);
                    }

                    produced++;
                }

                if (consumer.RecordsAffected > 0)
                {
                    if (!Channel.WaitForConfirms())
                    {
                        throw new Exception("WaitForConfirms error");
                    }
                }

                if (_tracker.HasErrors())
                {
                    throw new Exception("Delivery failure");
                }

                if (_useDeliveryTracking)
                {
                    _eventTracker?.FlushEvents();
                }

                consumer.TxCommit();
            }
            while (consumer.RecordsAffected > 0);

            return produced;
        }
        private ReadOnlyMemory<byte> GetMessageBody(in string message)
        {
            int bufferSize = message.Length * 2; // char == 2 bytes

            if (_buffer == null)
            {
                _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            }
            else if (_buffer.Length < bufferSize)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            }

            int encoded = Encoding.UTF8.GetBytes(message, 0, message.Length, _buffer, 0);

            ReadOnlyMemory<byte> messageBody = new ReadOnlyMemory<byte>(_buffer, 0, encoded);

            return messageBody;
        }
        private ReadOnlyMemory<byte> GetMessageBody(in OutgoingMessageDataMapper message) // in EntityJsonSerializer serializer
        {
            int bufferSize = message.MessageBody.Length * 2; // char == 2 bytes

            if (_buffer == null)
            {
                _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            }
            else if (_buffer.Length < bufferSize)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            }

            int encoded = Encoding.UTF8.GetBytes(message.MessageBody, 0, message.MessageBody.Length, _buffer, 0);

            ReadOnlyMemory<byte> messageBody = new ReadOnlyMemory<byte>(_buffer, 0, encoded);
            
            return messageBody;
        }

        #region "MESSAGE CONFIRMATION HANDLERS"

        private void BasicAcksHandler(object sender, BasicAckEventArgs args)
        {
            _tracker?.SetAckStatus(args.DeliveryTag, args.Multiple);

            if (_useDeliveryTracking)
            {
                _eventTracker?.SetAckStatus(args.DeliveryTag, args.Multiple);
            }
        }
        private void BasicNacksHandler(object sender, BasicNackEventArgs args)
        {
            _tracker?.SetNackStatus(args.DeliveryTag, args.Multiple);

            if (_useDeliveryTracking)
            {
                _eventTracker?.SetNackStatus(args.DeliveryTag, args.Multiple);
            }
        }
        private string GetReturnReason(in BasicReturnEventArgs args)
        {
            return "Message return (" + args.ReplyCode.ToString() + "): " +
                (string.IsNullOrWhiteSpace(args.ReplyText) ? "(empty)" : args.ReplyText) + ". " +
                "Exchange: " + (string.IsNullOrWhiteSpace(args.Exchange) ? "(empty)" : args.Exchange) + ". " +
                "RoutingKey: " + (string.IsNullOrWhiteSpace(args.RoutingKey) ? "(empty)" : args.RoutingKey) + ".";
        }
        private void BasicReturnHandler(object sender, BasicReturnEventArgs args)
        {
            if (_useDeliveryTracking &&
                args.BasicProperties != null &&
                args.BasicProperties.IsMessageIdPresent())
            {
                _eventTracker?.SetReturnStatus(args.BasicProperties.MessageId, GetReturnReason(in args));
            }

            if (_tracker != null && _tracker.IsReturned)
            {
                return; // already marked as returned
            }

            string reason = GetReturnReason(in args);

            if (args.BasicProperties != null &&
                args.BasicProperties.Headers != null &&
                args.BasicProperties.Headers.TryGetValue("CC", out object value) &&
                value != null &&
                value is List<object> recipients &&
                recipients != null &&
                recipients.Count > 0)
            {
                string cc = string.Empty;

                for (int i = 0; i < recipients.Count; i++)
                {
                    if (i == 10)
                    {
                        cc += ",...";

                        break;
                    }

                    if (recipients[i] is byte[] recipient)
                    {
                        if (string.IsNullOrEmpty(cc))
                        {
                            cc = Encoding.UTF8.GetString(recipient);
                        }
                        else
                        {
                            cc += "," + Encoding.UTF8.GetString(recipient);
                        }
                    }
                }

                if (!string.IsNullOrEmpty(cc))
                {
                    reason += " CC: " + cc;
                }
            }

            _tracker?.SetReturnedStatus(reason);
        }
        private void ModelShutdownHandler(object sender, ShutdownEventArgs args)
        {
            string reason = $"Channel shutdown ({args.ReplyCode}): {args.ReplyText}";
            _tracker?.SetShutdownStatus(reason);

            if (_useDeliveryTracking)
            {
                _eventTracker?.SetShutdownStatus(args.ToString());
            }
        }

        #endregion

        #region "CONFIGURE OUTGOING MESSAGE"

        private string GetHeaderVector(in IBasicProperties headers)
        {
            if (headers != null && headers.Headers != null)
            {
                if (headers.Headers.TryGetValue("vector", out object value))
                {
                    if (value is string vector1)
                    {
                        return vector1;
                    }
                    else if (value is byte[] vector2)
                    {
                        return Encoding.UTF8.GetString(vector2);
                    }
                }
            }
            return string.Empty;
        }
        private void SetVectorHeader(long vector, IBasicProperties properties)
        {
            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }

            if (!properties.Headers.TryAdd("vector", vector.ToString()))
            {
                properties.Headers["vector"] = vector.ToString();
            }
        }

        private void ConfigureMessageProperties(in OutgoingMessageDataMapper message, IBasicProperties properties, in OutMessageInfo deliveryInfo)
        {
            if (message is V1.OutgoingMessage message1)
            {
                ConfigureMessageProperties(in message1, properties, in deliveryInfo);
            }
            else if (message is V10.OutgoingMessage message10)
            {
                ConfigureMessageProperties(in message10, properties, in deliveryInfo);
            }
            else if (message is V11.OutgoingMessage message11)
            {
                ConfigureMessageProperties(in message11, properties, in deliveryInfo);
            }
            else if (message is V12.OutgoingMessage message12)
            {
                ConfigureMessageProperties(in message12, properties, in deliveryInfo);
            }
        }

        private void ConfigureMessageProperties(in V10.OutgoingMessage message, IBasicProperties properties, in OutMessageInfo deliveryInfo)
        {
            if (deliveryInfo != null)
            {
                deliveryInfo.MsgUid = message.Uuid;
                deliveryInfo.Type = message.MessageType;
                deliveryInfo.AppId = message.Sender;
                deliveryInfo.Recipients = message.Recipients;
                deliveryInfo.EventSelect = DateTime.UtcNow;
                deliveryInfo.EventType = DeliveryEventTypes.DBRMQ_SELECT;
                deliveryInfo.Vector = message.MessageNumber.ToString();
            }

            properties.AppId = message.Sender;
            properties.Type = message.MessageType;
            properties.MessageId = message.Uuid.ToString();

            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }
            else
            {
                properties.Headers.Clear();
            }

            SetVectorHeader(message.MessageNumber, properties);

            SetOperationTypeHeader(message, properties);

            if (_exchangeRole == ExchangeRoles.Aggregator)
            {
                SetAggregatorCopyHeader(message, properties);
            }
            else if (_exchangeRole == ExchangeRoles.Dispatcher)
            {
                SetDispatcherCopyHeader(message, properties);
            }
        }
        private void SetOperationTypeHeader(in V10.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.OperationType)) return;

            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }

            if (!properties.Headers.TryAdd("OperationType", message.OperationType))
            {
                properties.Headers["OperationType"] = message.OperationType;
            }
        }
        private void SetAggregatorCopyHeader(in V10.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.Sender)) return;

            properties.Headers.Add("CC", new string[] { message.Sender });
        }
        private void SetDispatcherCopyHeader(in V10.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.Recipients)) return;

            properties.Headers.Add("CC", message.Recipients.Split(',', StringSplitOptions.RemoveEmptyEntries));
        }

        private void ConfigureMessageProperties(in V11.OutgoingMessage message, IBasicProperties properties, in OutMessageInfo deliveryInfo)
        {
            if (deliveryInfo != null)
            {
                deliveryInfo.MsgUid = message.Uuid;
                deliveryInfo.Type = message.MessageType;
                deliveryInfo.AppId = message.Sender;
                deliveryInfo.Recipients = message.Recipients;
                deliveryInfo.EventSelect = DateTime.UtcNow;
                deliveryInfo.EventType = DeliveryEventTypes.DBRMQ_SELECT;
                deliveryInfo.Vector = message.MessageNumber.ToString();
            }

            properties.AppId = message.Sender;
            properties.Type = message.MessageType;
            properties.MessageId = message.Uuid.ToString();

            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }
            else
            {
                properties.Headers.Clear();
            }

            SetVectorHeader(message.MessageNumber, properties);

            SetOperationTypeHeader(message, properties);

            if (_exchangeRole == ExchangeRoles.Aggregator)
            {
                SetAggregatorCopyHeader(message, properties);
            }
            else if (_exchangeRole == ExchangeRoles.Dispatcher)
            {
                SetDispatcherCopyHeader(message, properties);
            }

            if (!string.IsNullOrWhiteSpace(message.Headers))
            {
                try
                {
                    Dictionary<string, string> headers = JsonSerializer.Deserialize<Dictionary<string, string>>(message.Headers);
                    foreach (var header in headers)
                    {
                        _ = properties.Headers.TryAdd(header.Key, header.Value);
                    }
                }
                catch (Exception error)
                {
                    throw new FormatException($"Message headers format exception. Message number: {{{message.MessageNumber}}}. Error message: {error.Message}");
                }
            }
        }
        private void SetOperationTypeHeader(in V11.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.OperationType)) return;

            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }

            if (!properties.Headers.TryAdd("OperationType", message.OperationType))
            {
                properties.Headers["OperationType"] = message.OperationType;
            }
        }
        private void SetAggregatorCopyHeader(in V11.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.Sender)) return;

            properties.Headers.Add("CC", new string[] { message.Sender });
        }
        private void SetDispatcherCopyHeader(in V11.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.Recipients)) return;

            properties.Headers.Add("CC", message.Recipients.Split(',', StringSplitOptions.RemoveEmptyEntries));
        }

        private void ConfigureMessageProperties(in V12.OutgoingMessage message, IBasicProperties properties, in OutMessageInfo deliveryInfo)
        {
            if (deliveryInfo != null)
            {
                deliveryInfo.MsgUid = message.Uuid;
                deliveryInfo.Type = message.MessageType;
                deliveryInfo.AppId = message.Sender;
                deliveryInfo.Recipients = message.Recipients;
                deliveryInfo.EventSelect = DateTime.UtcNow;
                deliveryInfo.EventType = DeliveryEventTypes.DBRMQ_SELECT;
                deliveryInfo.Vector = message.MessageNumber.ToString();
            }

            properties.AppId = message.Sender;
            properties.Type = message.MessageType;
            properties.MessageId = message.Uuid.ToString();

            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }
            else
            {
                properties.Headers.Clear();
            }

            SetVectorHeader(message.MessageNumber, properties);

            if (_exchangeRole == ExchangeRoles.Aggregator)
            {
                SetAggregatorCopyHeader(message, properties);
            }
            else if (_exchangeRole == ExchangeRoles.Dispatcher)
            {
                SetDispatcherCopyHeader(message, properties);
            }

            if (!string.IsNullOrWhiteSpace(message.Headers))
            {
                try
                {
                    Dictionary<string, string> headers = JsonSerializer.Deserialize<Dictionary<string, string>>(message.Headers);
                    foreach (var header in headers)
                    {
                        _ = properties.Headers.TryAdd(header.Key, header.Value);
                    }
                }
                catch (Exception error)
                {
                    throw new FormatException($"Message headers format exception. Message number: {{{message.MessageNumber}}}. Error message: {error.Message}");
                }
            }
        }
        private void SetAggregatorCopyHeader(in V12.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.Sender)) return;

            properties.Headers.Add("CC", new string[] { message.Sender });
        }
        private void SetDispatcherCopyHeader(in V12.OutgoingMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.Recipients)) return;

            properties.Headers.Add("CC", message.Recipients.Split(',', StringSplitOptions.RemoveEmptyEntries));
        }

        private void ConfigureMessageProperties(in V1.OutgoingMessage message, IBasicProperties properties, in OutMessageInfo deliveryInfo)
        {
            if (deliveryInfo != null)
            {
                deliveryInfo.MsgUid = message.Uuid;
                deliveryInfo.Type = message.MessageType;
                deliveryInfo.AppId = Options.Value.ThisNode;
                deliveryInfo.Recipients = string.Empty;
                deliveryInfo.EventSelect = DateTime.UtcNow;
                deliveryInfo.EventType = DeliveryEventTypes.DBRMQ_SELECT;
                deliveryInfo.Vector = message.MessageNumber.ToString();
            }

            properties.Type = message.MessageType;
            properties.MessageId = message.Uuid.ToString();

            if (!string.IsNullOrWhiteSpace(message.Headers))
            {
                try
                {
                    Dictionary<string, string> headers = JsonSerializer.Deserialize<Dictionary<string, string>>(message.Headers);
                    ConfigureMessageHeaders(in headers, properties);
                }
                catch (Exception error)
                {
                    throw new FormatException($"Message headers format exception. Message number: {{{message.MessageNumber}}}. Error message: {error.Message}");
                }
            }

            SetVectorHeader(message.MessageNumber, properties);
        }
        private void ConfigureMessageHeaders(in Dictionary<string, string> headers, IBasicProperties properties)
        {
            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }
            else
            {
                properties.Headers.Clear();
            }

            foreach (var header in headers)
            {
                if (header.Key == "CC")
                {
                    _ = properties.Headers.TryAdd("CC", header.Value.Split(',', StringSplitOptions.RemoveEmptyEntries));
                }
                else if (header.Key == "BCC")
                {
                    _ = properties.Headers.TryAdd("BCC", header.Value.Split(',', StringSplitOptions.RemoveEmptyEntries));
                }
                else if (header.Key == "Sender")
                {
                    properties.AppId = header.Value;
                    _ = properties.Headers.TryAdd("Sender", header.Value);
                }
                else
                {
                    _ = properties.Headers.TryAdd(header.Key, header.Value);
                }
            }
        }

        #endregion

        #region "DELIVERY SEQUENCE CONTROL"

        private void ValidateVector(in IBasicProperties headers, ReadOnlyMemory<byte> message)
        {
            if (headers == null)
            {
                return;
            }

            try
            {
                TryValidateVector(headers, message);
            }
            catch (Exception error)
            {
                FileLogger.Log(ExceptionHelper.GetErrorText(error));
            }
        }
        private void TryValidateVector(in IBasicProperties headers, ReadOnlyMemory<byte> message)
        {
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

            string key = MessageJsonParser.ExtractEntityKey(type, message);
            if (string.IsNullOrEmpty(key)) { return; }

            _ = _vectorService?.ValidateVector(node, type, key, vector);
        }

        #endregion

        #region "PUBLISH DELIVERY TRACKING EVENTS"

        public int PublishDeliveryTrackingEvents()
        {
            int consumed;
            int published = 0;

            do
            {
                _tracker = new PublishTracker();

                consumed = _eventTracker.ProcessEvents(this);

                published += consumed;
            }
            while (consumed > 0);

            return published;
        }
        public void Process(DeliveryEvent @event)
        {
            if (ConnectionIsBlocked)
            {
                throw new Exception("Connection is blocked");
            }

            Properties.AppId = @event.EventNode;
            Properties.Type = @event.EventType;
            Properties.MessageId = @event.MsgUid.ToString().ToLower();
            
            if (Properties.Headers != null)
            {
                Properties.Headers.Clear();
            }

            string message = JsonSerializer.Serialize(@event, typeof(DeliveryEvent), _serializerOptions);

            ReadOnlyMemory<byte> messageBody = GetMessageBody(in message);

            _tracker.Track(Channel.NextPublishSeqNo);

            if (string.IsNullOrWhiteSpace(RoutingKey))
            {
                Channel.BasicPublish(ExchangeName, @event.EventNode, false, Properties, messageBody);
            }
            else
            {
                Channel.BasicPublish(ExchangeName, RoutingKey, false, Properties, messageBody);
            }
        }
        public void Synchronize()
        {
            if (!Channel.WaitForConfirms())
            {
                throw new Exception("[DeliveryTracking] WaitForConfirms error");
            }

            if (_tracker.HasErrors())
            {
                throw new Exception("[DeliveryTracking] Delivery failure");
            }

            _tracker.Clear();
        }

        #endregion
    }
}