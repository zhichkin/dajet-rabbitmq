using DaJet.Data.Mapping;
using DaJet.Data.Messaging;
using DaJet.Json;
using DaJet.Logging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using DaJet.RabbitMQ.Handlers;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using SqlServer = DaJet.Data.Messaging.SqlServer;

namespace DaJet.RabbitMQ.Test
{
    [TestClass] public class UsageTests
    {
        private const string INCOMING_QUEUE_NAME = "–егистр—ведений.“естова€¬ход€ща€ќчередь"; //"–егистр—ведений.¬ход€ща€ќчередь12";
        private const string OUTGOING_QUEUE_NAME = "–егистр—ведений.»сход€ща€ќчередь11";
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=localhost;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";

        [TestMethod] public void TestRmqMessageProducer()
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            if (!new MetadataService()
                .UseConnectionString(MS_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }
            
            ApplicationObject queue = infoBase.GetApplicationObjectByName(OUTGOING_QUEUE_NAME);
            if (queue == null)
            {
                Console.WriteLine($"ќбъект метаданных \"{OUTGOING_QUEUE_NAME}\" не найден."); return;
            }
            Console.WriteLine($"{queue.Name} [{queue.TableName}]");

            int version = GetOutgoingDataContractVersion(in queue);
            if (version < 1) { return; }
            Console.WriteLine($"¬ерси€ исход€щей очереди: {version}");

            if (!ConfigureOutgoingQueue(version, in queue))
            {
                return;
            }
            Console.WriteLine();

            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            string routingKey = "—правочник.“естовый—правочник";
            string uri = "amqp://guest:guest@localhost:5672/%2F/DISPATCHER";
            //string uri = "amqp://guest:guest@localhost:5672/%2F/AGGREGATOR";

            using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in queue))
            {
                using (RmqMessageProducer producer = new RmqMessageProducer(uri, routingKey))
                {
                    producer.Initialize(ExchangeRoles.Dispatcher);

                    int published = producer.Publish(consumer, serializer);

                    Console.WriteLine($"Published {published} messages.");
                }
            }

            watch.Stop();
            Console.WriteLine($"Elapsed in {watch.ElapsedMilliseconds} ms");
        }
        private int GetOutgoingDataContractVersion(in ApplicationObject queue)
        {
            DbInterfaceValidator validator = new DbInterfaceValidator();
            int version = validator.GetOutgoingInterfaceVersion(in queue);
            if (version < 1)
            {
                Console.WriteLine($"Ќе удалось определить версию контракта данных.");
            }
            return version;
        }
        private bool ConfigureOutgoingQueue(int version, in ApplicationObject queue)
        {
            DbQueueConfigurator configurator = new DbQueueConfigurator(version, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);
            configurator.ConfigureOutgoingMessageQueue(in queue, out List<string> errors);

            if (errors.Count > 0)
            {
                foreach (string error in errors)
                {
                    Console.WriteLine(error);
                }

                return false;
            }

            Console.WriteLine($"»сход€ща€ очередь настроена успешно.");

            return true;
        }

        [TestMethod] public void TestRmqMessageConsumer()
        {
            string uri = "amqp://guest:guest@localhost:5672/%2F";
            List<string> queues = new List<string>()
            {
                "–»Ѕ.MAIN.N001",
                "–»Ѕ.MAIN.N002"
            };

            CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            using (RmqMessageConsumer consumer = new RmqMessageConsumer(uri, in queues))
            {
                consumer.Initialize(DatabaseProvider.SQLServer, MS_CONNECTION_STRING, INCOMING_QUEUE_NAME);

                Console.WriteLine($"Host: {consumer.HostName}");
                Console.WriteLine($"Port: {consumer.HostPort}");
                Console.WriteLine($"User: {consumer.UserName}");
                Console.WriteLine($"Pass: {consumer.Password}");
                Console.WriteLine($"VHost: {consumer.VirtualHost}");

                consumer.Consume(stop.Token, FileLogger.Log);
            }
        }

        [TestMethod] public void Show_Consumer_Settings_MS()
        {
            if (!new MetadataService()
                .UseConnectionString(MS_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ExchangePlanHelper settings = new ExchangePlanHelper(in infoBase, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);
            settings.ConfigureSelectScripts("ѕланќбмена.DaJetMessaging", "–егистр—ведений.Ќастройкиќбмена–»Ѕ");
            List<string> queues = settings.GetIncomingQueueNames();

            foreach (string name in queues)
            {
                Console.WriteLine(name);
            }
        }
        [TestMethod] public void Show_Consumer_Settings_PG()
        {
            if (!new MetadataService()
                .UseConnectionString(PG_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ExchangePlanHelper settings = new ExchangePlanHelper(in infoBase, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);
            settings.ConfigureSelectScripts("ѕланќбмена.DaJetMessaging", "–егистр—ведений.Ќастройкиќбмена–»Ѕ");
            List<string> queues = settings.GetIncomingQueueNames();

            foreach (string name in queues)
            {
                Console.WriteLine(name);
            }
        }

        [TestMethod] public void NEW_ProducerHandler()
        {
            MessageBrokerOptions options = new MessageBrokerOptions()
            {
                RoutingKey = "dajet-queue"
            };

            DatabaseMessage message = new DatabaseMessage()
            {
                MessageNumber = 1L,
                Headers = "{\"Sender\":\"DaJet.RabbitMQ\",\"CC\":\"N001,N002\"}",
                MessageType = "—правочник.Ќоменклатура",
                MessageBody = "{\"test\":\"сообщение\"}"
            };

            using (var handler = new Handlers.RmqMessageProducer(Options.Create(options)))
            {
                handler.Initialize();

                handler.Handle(in message);

                if (handler.Confirm())
                {
                    Console.WriteLine($"Confirmed");
                }
            }
        }
        [TestMethod] public void NEW_Consumer_ProducerHandler()
        {
            if (!new MetadataService()
                .UseConnectionString(MS_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName($"–егистр—ведений.»сход€ща€ќчередь1");

            DatabaseConsumerOptions db_options = new DatabaseConsumerOptions()
            {
                QueueTableName = queue.TableName,
                ConnectionString = MS_CONNECTION_STRING,
                MessagesPerTransaction = 1
            };
            foreach (MetadataProperty property in queue.Properties)
            {
                db_options.TableColumns.Add(property.Name, property.Fields[0].Name);
            }

            CancellationTokenSource source = new CancellationTokenSource();

            IDbMessageConsumer consumer = new SqlServer.MsMessageConsumer(Options.Create(db_options));

            MessageBrokerOptions mb_options = new MessageBrokerOptions()
            {
                ExchangeName = "–»Ѕ.ERP",
                ExchangeRole = ExchangeRoles.Dispatcher
            };

            var handler = new Handlers.RmqMessageProducer(Options.Create(mb_options));

            using (handler)
            {
                handler.Initialize();
                consumer.Consume(handler, source.Token);
            }
        }
    }
}