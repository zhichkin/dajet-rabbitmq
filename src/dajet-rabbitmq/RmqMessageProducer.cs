using DaJet.Data.Messaging;
using DaJet.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Web;

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

        private string _AppId = string.Empty;
        private string _MessageType = string.Empty;

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
        public string AppId
        {
            get { return _AppId; }
            set
            {
                _AppId = value;
                Properties.AppId = _AppId;
            }
        }
        public string MessageType
        {
            get { return _MessageType; }
            set
            {
                _MessageType = value;
                Properties.Type = _MessageType;
            }
        }

        #region "RABBITMQ CONNECTION SETUP"

        public void Initialize()
        {
            Connection = CreateConnection();
            Channel = CreateChannel(Connection);
            Properties = CreateMessageProperties(Channel);
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
        private IBasicProperties CreateMessageProperties(IModel channel)
        {
            IBasicProperties properties = channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent
            properties.ContentEncoding = "UTF-8";
            properties.AppId = AppId;
            properties.Type = MessageType;
            SetOperationTypeHeader(properties);
            return properties;
        }
        private void SetOperationTypeHeader(IBasicProperties properties)
        {
            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }

            if (!properties.Headers.TryAdd("OperationType", "UPSERT"))
            {
                properties.Headers["OperationType"] = "UPSERT";
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

                    Properties.Type = message.MessageType;
                    Properties.MessageId = Guid.NewGuid().ToString();
                    
                    ReadOnlyMemory<byte> messageBody = message.GetMessageBody();
                    if (messageBody.IsEmpty)
                    {
                        if (message is Data.Messaging.V3.OutgoingMessage msg)
                        {
                            messageBody = serializer.Serialize(msg.Reference);
                        }
                    }

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
    }
}