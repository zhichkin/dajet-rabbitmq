using DaJet.Data.Messaging;
using DaJet.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Web;
using V10 = DaJet.Data.Messaging.V10;
using V11 = DaJet.Data.Messaging.V11;
using V12 = DaJet.Data.Messaging.V12;

namespace DaJet.RabbitMQ
{
    public sealed class RmqMessageProducer : IDisposable
    {
        private IConnection Connection;
        private IModel Channel;
        private IBasicProperties Properties;
        private bool ConnectionIsBlocked = false;
        
        private bool IsNacked = false;
        private ulong DeliveryTag = 0UL;

        private byte[] _buffer; // message body buffer
        private ExchangeRoles _exchangeRole = ExchangeRoles.None;

        public string HostName { get; private set; } = "localhost";
        public int HostPort { get; private set; } = 5672;
        public string VirtualHost { get; private set; } = "/";
        public string UserName { get; private set; } = "guest";
        public string Password { get; private set; } = "guest";
        public string ExchangeName { get; private set; } = string.Empty; // if empty RoutingKey is a queue name to send directly
        public string RoutingKey { get; private set; } = string.Empty; // if exchange name is not empty this is routing key value

        public RmqMessageProducer(string uri, string routingKey)
        {
            ParseRmqUri(uri);
            RoutingKey = routingKey;
        }

        #region "RABBITMQ CONNECTION SETUP"

        public void Initialize(ExchangeRoles role)
        {
            Connection = CreateConnection();
            Channel = CreateChannel(Connection);
            Properties = CreateMessageProperties(Channel);
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
        private void BasicAcksHandler(object sender, BasicAckEventArgs args)
        {
            //if (!(sender is IModel channel)) return;

            DeliveryTag = args.DeliveryTag;
        }
        private void BasicNacksHandler(object sender, BasicNackEventArgs args)
        {
            if (args.DeliveryTag <= DeliveryTag)
            {
                IsNacked = true;
            }
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

        public int Publish(IMessageConsumer consumer, EntityJsonSerializer serializer)
        {
            int consumed = 0;
            int produced = 0;

            do
            {
                consumed = 0;

                consumer.TxBegin();
                foreach (OutgoingMessageDataMapper message in consumer.Select())
                {
                    if (ConnectionIsBlocked)
                    {
                        throw new Exception("Connection is blocked");
                    }

                    ConfigureMessageProperties(in message, Properties);

                    ReadOnlyMemory<byte> messageBody = GetMessageBody(in message, in serializer);

                    Channel.BasicPublish(ExchangeName, RoutingKey, Properties, messageBody);
                    
                    produced++;
                }
                consumer.TxCommit();

                consumed = consumer.RecordsAffected;

                if (consumed > 0)
                {
                    if (!Channel.WaitForConfirms())
                    {
                        throw new Exception("WaitForConfirms error");
                    }
                }
            }
            while (consumer.RecordsAffected > 0);

            return produced;
        }
        
        private ReadOnlyMemory<byte> GetMessageBody(in OutgoingMessageDataMapper message, in EntityJsonSerializer serializer)
        {
            int bufferSize = message.MessageBody.Length * 2; // char == 2 bytes

            if (_buffer != null && _buffer.Length < bufferSize)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
            }
            else
            {
                _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            }
            
            int encoded = Encoding.UTF8.GetBytes(message.MessageBody, 0, message.MessageBody.Length, _buffer, 0);

            ReadOnlyMemory<byte> messageBody = new ReadOnlyMemory<byte>(_buffer, 0, encoded);

            if (messageBody.IsEmpty)
            {
                if (message is V11.OutgoingMessage message11)
                {
                    messageBody = serializer.Serialize(message.MessageType, message11.Reference);

                    if (messageBody.IsEmpty)
                    {
                        messageBody = serializer.SerializeAsObjectDeletion(message.MessageType, message11.Reference);
                    }
                }
                else if (message is V12.OutgoingMessage message12)
                {
                    messageBody = serializer.Serialize(message.MessageType, message12.Reference);

                    if (messageBody.IsEmpty)
                    {
                        messageBody = serializer.SerializeAsObjectDeletion(message.MessageType, message12.Reference);
                    }
                }
            }
            
            return messageBody;
        }
        
        private void ConfigureMessageProperties(in OutgoingMessageDataMapper message, IBasicProperties properties)
        {
            if (message is V10.OutgoingMessage message10)
            {
                ConfigureMessageProperties(in message10, properties);
            }
            else if (message is V11.OutgoingMessage message11)
            {
                ConfigureMessageProperties(in message11, properties);
            }
            else if (message is V12.OutgoingMessage message12)
            {
                ConfigureMessageProperties(in message12, properties);
            }
        }

        private void ConfigureMessageProperties(in V10.OutgoingMessage message, IBasicProperties properties)
        {
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

        private void ConfigureMessageProperties(in V11.OutgoingMessage message, IBasicProperties properties)
        {
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
        }
        private void ConfigureMessageProperties(in V12.OutgoingMessage message, IBasicProperties properties)
        {
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
                if (header.Key == "Sender")
                {
                    properties.AppId = header.Value;

                    if (_exchangeRole == ExchangeRoles.Aggregator)
                    {
                        _ = properties.Headers.TryAdd("CC", new string[] { header.Value });
                    }
                }
                else if (header.Key == "Recipients")
                {
                    if (_exchangeRole == ExchangeRoles.Aggregator)
                    {
                        continue;
                    }
                    else if (_exchangeRole == ExchangeRoles.Dispatcher)
                    {
                        _ = properties.Headers.TryAdd("CC", header.Value.Split(',', StringSplitOptions.RemoveEmptyEntries));
                    }
                    else
                    {
                        _ = properties.Headers.TryAdd(header.Key, header.Value);
                    }
                }
                else if (header.Key == "CC")
                {
                    _ = properties.Headers.TryAdd("CC", header.Value.Split(',', StringSplitOptions.RemoveEmptyEntries));
                }
                else if (header.Key == "BCC")
                {
                    _ = properties.Headers.TryAdd("BCC", header.Value.Split(',', StringSplitOptions.RemoveEmptyEntries));
                }
                else
                {
                    _ = properties.Headers.TryAdd(header.Key, header.Value);
                }
            }
        }
    }
}