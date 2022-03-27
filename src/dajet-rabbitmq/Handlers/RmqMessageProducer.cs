using DaJet.Data.Messaging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace DaJet.RabbitMQ.Handlers
{
    public sealed class RmqMessageProducer : DbMessageHandler, IDisposable
    {
        private IConnection _connection;
        private IModel _channel;
        private IBasicProperties _properties;
        private bool _connectionIsBlocked = false;
        private byte[] _buffer; // message body buffer
        private readonly MessageBrokerOptions _options;
        
        public RmqMessageProducer(IOptions<MessageBrokerOptions> options)
        {
            _options = options.Value;
        }
        public void Initialize()
        {
            _connection = CreateConnection();
            _channel = CreateChannel(_connection);
            _properties = CreateMessageProperties(_channel);
        }
        public void Dispose()
        {
            _channel?.Dispose();
            _channel = null;

            _connection?.Dispose();
            _connection = null;

            if (_buffer != null)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
            }
        }
        public override void Handle(in DatabaseMessage message)
        {
            if (_connectionIsBlocked)
            {
                throw new Exception("Connection is blocked");
            }

            base.Handle(in message);

            ConfigureMessageProperties(in message, _properties);

            ReadOnlyMemory<byte> messageBody = GetMessageBody(in message);

            if (string.IsNullOrWhiteSpace(_options.RoutingKey))
            {
                _channel.BasicPublish(_options.ExchangeName, message.MessageType, _properties, messageBody);
            }
            else
            {
                _channel.BasicPublish(_options.ExchangeName, _options.RoutingKey, _properties, messageBody);
            }
        }
        public override bool Confirm()
        {
            return _channel.WaitForConfirms();
        }

        #region "RABBITMQ CONNECTION SETUP"

        private IConnection CreateConnection()
        {
            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = _options.HostName,
                Port = _options.HostPort,
                VirtualHost = _options.VirtualHost,
                UserName = _options.UserName,
                Password = _options.Password
            };
            IConnection connection = factory.CreateConnection();
            connection.ConnectionBlocked += HandleConnectionBlocked;
            connection.ConnectionUnblocked += HandleConnectionUnblocked;
            return connection;
        }
        private void HandleConnectionBlocked(object sender, ConnectionBlockedEventArgs args)
        {
            _connectionIsBlocked = true;
        }
        private void HandleConnectionUnblocked(object sender, EventArgs args)
        {
            _connectionIsBlocked = false;
        }
        private IModel CreateChannel(IConnection connection)
        {
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
            // TODO
        }
        private void BasicNacksHandler(object sender, BasicNackEventArgs args)
        {
            // TODO
        }

        #endregion

        private ReadOnlyMemory<byte> GetMessageBody(in DatabaseMessage message)
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
        private Dictionary<string, string> GetMessageHeaders(in DatabaseMessage message)
        {
            Dictionary<string, string> headers = null;

            if (!string.IsNullOrWhiteSpace(message.Headers))
            {
                try
                {
                    headers = JsonSerializer.Deserialize<Dictionary<string, string>>(message.Headers);
                }
                catch
                {
                    // TODO: log error
                }
            }

            return headers;
        }
        private void ConfigureMessageProperties(in DatabaseMessage message, IBasicProperties properties)
        {
            properties.Type = message.MessageType;
            properties.MessageId = message.MessageNumber.ToString();

            if (!string.IsNullOrWhiteSpace(message.Headers))
            {
                Dictionary<string, string> headers = GetMessageHeaders(in message);
                
                if (headers != null)
                {
                    ConfigureMessageHeaders(in headers, properties);
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

                    if (_options.ExchangeRole == ExchangeRoles.Aggregator)
                    {
                        _ = properties.Headers.TryAdd("CC", new string[] { header.Value });
                    }
                }
                else if (header.Key == "Recipients")
                {
                    if (_options.ExchangeRole == ExchangeRoles.Aggregator)
                    {
                        continue;
                    }
                    else if (_options.ExchangeRole == ExchangeRoles.Dispatcher)
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

    public sealed class Utf8MessageBodyEncoder : DbMessageHandler
    {
        public override void Handle(in DatabaseMessage message)
        {
            int bufferSize = message.MessageBody.Length * 2; // char == 2 bytes

            //if (_buffer == null)
            //{
            //    _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            //}
            //else if (_buffer.Length < bufferSize)
            //{
            //    ArrayPool<byte>.Shared.Return(_buffer);
            //    _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            //}

            //int encoded = Encoding.UTF8.GetBytes(message.MessageBody, 0, message.MessageBody.Length, _buffer, 0);

            //ReadOnlyMemory<byte> messageBody = new ReadOnlyMemory<byte>(_buffer, 0, encoded);
        }
    }
}