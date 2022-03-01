using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace DaJet.RabbitMQ
{
    public sealed class RmqMessageConsumer : IDisposable
    {
        private IConnection Connection;
        private ConcurrentDictionary<string, EventingBasicConsumer> Consumers = new ConcurrentDictionary<string, EventingBasicConsumer>();

        public string HostName { get; private set; } = "localhost";
        public int HostPort { get; private set; } = 5672;
        public string VirtualHost { get; private set; } = "/";
        public string UserName { get; private set; } = "guest";
        public string Password { get; private set; } = "guest";

        public RmqMessageConsumer(in string uri)
        {
            ParseRmqUri(in uri);
        }
        public void Consume(in List<string> queues)
        {
            Connection = CreateConnection();
            InitializeConsumers(in queues);

            Task.Delay(TimeSpan.FromSeconds(60)).Wait();
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

        private void DisposeConsumer(string consumerTag)
        {
            if (!Consumers.TryGetValue(consumerTag, out EventingBasicConsumer consumer))
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

                consumer = null;
            }
            
            _ = Consumers.TryRemove(consumerTag, out _);
        }
        private void InitializeConsumers(in List<string> queues)
        {
            foreach (string queue in queues)
            {
                StartConsumerTask(queue);
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

            _ = Consumers.TryAdd(consumerTag, consumer);
        }
        private bool IsConsumerHealthy(EventingBasicConsumer consumer)
        {
            return (consumer != null
                && consumer.Model != null
                && consumer.Model.IsOpen
                && consumer.IsRunning);
        }

        private void ProcessMessage(object sender, BasicDeliverEventArgs args)
        {
            if (!(sender is EventingBasicConsumer consumer)) return;

            consumer.Model.BasicAck(args.DeliveryTag, false);

            //if (!Consumers.TryGetValue(args.ConsumerTag, out _))
            //{

            //}

            string exchangeName = GetExchangeName(in args);

            string messageBody = Encoding.UTF8.GetString(args.Body.Span);

            Console.WriteLine($"{exchangeName}: {messageBody}");


            //JsonDataTransferMessage dataTransferMessage = GetJsonDataTransferMessage(args);
            //if (dataTransferMessage == null)
            //{
            //    RemovePoisonMessage(exchange, consumer, args.DeliveryTag);
            //    return;
            //}

            //bool success = true;
            //IDatabaseMessageProducer producer = Services.GetService<IDatabaseMessageProducer>();
            //try
            //{
            //    DatabaseMessage message = producer.ProduceMessage(dataTransferMessage);
            //    success = producer.InsertMessage(message);
            //    if (success)
            //    {
            //        consumer.Model.BasicAck(args.DeliveryTag, false);
            //    }
            //}
            //catch (Exception error)
            //{
            //    success = false;
            //    FileLogger.Log(LOG_TOKEN, ExceptionHelper.GetErrorText(error));
            //}

            //if (!success)
            //{
            //    // return unacked messages back to queue in the same order (!)
            //    ResetConsumer(args.ConsumerTag);

            //    FileLogger.Log(LOG_TOKEN,
            //        "Failed to process message. Consumer (tag = " + args.ConsumerTag.ToString()
            //        + ") for exchange \"" + exchange + "\" has been reset.");
            //}
        }
        private string GetExchangeName(in BasicDeliverEventArgs args)
        {
            if (args == null) return "Unknown";

            if (!string.IsNullOrWhiteSpace(args.Exchange))
            {
                return args.Exchange;
            }
            else if (!string.IsNullOrWhiteSpace(args.RoutingKey))
            {
                return args.RoutingKey;
            }
            
            return "Unknown";
        }
        private void RemovePoisonMessage(string exchange, EventingBasicConsumer consumer, ulong deliveryTag)
        {
            try
            {
                consumer.Model.BasicNack(deliveryTag, false, false);
                // TODO: FileLogger.Log("Poison message (bad format) has been removed from queue \"" + exchange + "\".");
            }
            catch
            {
                throw;
                //FileLogger.Log(LOG_TOKEN, ExceptionHelper.GetErrorText(error));
                //FileLogger.Log(LOG_TOKEN, "Failed to Nack message for exchange \"" + exchange + "\".");
            }
        }
        //private JsonDataTransferMessage GetJsonDataTransferMessage(BasicDeliverEventArgs args)
        //{
        //    string messageBody = Encoding.UTF8.GetString(args.Body.Span);

        //    JsonDataTransferMessage dataTransferMessage = null;

        //    if (string.IsNullOrWhiteSpace(args.BasicProperties.Type))
        //    {
        //        try
        //        {
        //            dataTransferMessage = JsonSerializer.Deserialize<JsonDataTransferMessage>(messageBody);
        //        }
        //        catch (Exception error)
        //        {
        //            FileLogger.Log(LOG_TOKEN, ExceptionHelper.GetErrorText(error));
        //        }
        //    }
        //    else
        //    {
        //        dataTransferMessage = new JsonDataTransferMessage()
        //        {
        //            Sender = (args.BasicProperties.AppId == null ? string.Empty : args.BasicProperties.AppId)
        //        };
        //        dataTransferMessage.Objects.Add(new JsonDataTransferObject()
        //        {
        //            Type = (args.BasicProperties.Type == null ? string.Empty : args.BasicProperties.Type),
        //            Body = messageBody,
        //            Operation = string.Empty
        //        });

        //        if (args.BasicProperties.Headers != null)
        //        {
        //            if (args.BasicProperties.Headers.TryGetValue("OperationType", out object value))
        //            {
        //                if (value is byte[] operationType)
        //                {
        //                    dataTransferMessage.Objects[0].Operation = Encoding.UTF8.GetString(operationType);
        //                }
        //            }
        //        }
        //    }

        //    return dataTransferMessage;
        //}
    }
}