using DaJet.Data.Mapping;
using DaJet.Data.Messaging;
using DaJet.Data.Messaging.V10;
using DaJet.Json;
using DaJet.Logging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using DaJet.Vector;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Timers;

namespace DaJet.RabbitMQ.Test
{
    [TestClass] public class UsageTests
    {
        private const string INCOMING_QUEUE_NAME = "РегистрСведений.ВходящаяОчередь10";
        private const string OUTGOING_QUEUE_NAME = "РегистрСведений.ИсходящаяОчередь11";
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
                Console.WriteLine($"Объект метаданных \"{OUTGOING_QUEUE_NAME}\" не найден."); return;
            }
            Console.WriteLine($"{queue.Name} [{queue.TableName}]");

            int version = GetOutgoingDataContractVersion(in queue);
            if (version < 1) { return; }
            Console.WriteLine($"Версия исходящей очереди: {version}");

            if (!ConfigureOutgoingQueue(version, in queue))
            {
                return;
            }
            Console.WriteLine();

            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            string routingKey = "Справочник.ТестовыйСправочник";
            string uri = "amqp://guest:guest@localhost:5672/%2F/DISPATCHER";
            //string uri = "amqp://guest:guest@localhost:5672/%2F/AGGREGATOR";

            using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in queue))
            {
                using (RmqMessageProducer producer = new RmqMessageProducer(uri, routingKey))
                {
                    producer.Initialize(ExchangeRoles.Dispatcher);

                    int published = 0;//producer.Publish(consumer, serializer);

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
                Console.WriteLine($"Не удалось определить версию контракта данных.");
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

            Console.WriteLine($"Исходящая очередь настроена успешно.");

            return true;
        }

        [TestMethod] public void TestRmqMessageConsumer()
        {
            string uri = "amqp://guest:guest@localhost:5672/%2F";
            
            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("rmq-test");

            _options = Options.Create(new RmqConsumerOptions()
            {
                Heartbeat = 10,
                Queues = GetIncomingQueueSettings()
            });

            StartConsumerOptionsUpdateService();

            CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(180));

            using (RmqMessageConsumer consumer = new RmqMessageConsumer(uri))
            {
                consumer.Configure(_options);

                consumer.Initialize(DatabaseProvider.SQLServer, MS_CONNECTION_STRING, INCOMING_QUEUE_NAME);

                Console.WriteLine($"Host: {consumer.HostName}");
                Console.WriteLine($"Port: {consumer.HostPort}");
                Console.WriteLine($"User: {consumer.UserName}");
                Console.WriteLine($"Pass: {consumer.Password}");
                Console.WriteLine($"VHost: {consumer.VirtualHost}");

                consumer.Consume(stop.Token, FileLogger.Log);
            }

            StopConsumerOptionsUpdateService();
        }
        
        private System.Timers.Timer _timer;
        private IOptions<RmqConsumerOptions> _options;
        private void StartConsumerOptionsUpdateService()
        {
            _timer = new System.Timers.Timer();
            _timer.Elapsed += UpdateConsumerOptions;
            _timer.Interval = _options.Value.Heartbeat * 1000;
            _timer.Start();
        }
        private void StopConsumerOptionsUpdateService()
        {
            _timer?.Stop();
            _timer?.Dispose();
        }
        private void UpdateConsumerOptions(object sender, ElapsedEventArgs args)
        {
            _options.Value.Queues = GetIncomingQueueSettings();
        }
        private List<string> GetIncomingQueueSettings()
        {
            List<string> queues = new List<string>();

            if (!new MetadataService()
                .UseConnectionString(MS_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return queues;
            }

            ExchangePlanHelper settings = new ExchangePlanHelper(in infoBase, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);
            settings.ConfigureSelectScripts("ПланОбмена.DaJetMessaging", "test.test");
            
            return settings.GetIncomingQueueNames();
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
            settings.ConfigureSelectScripts("ПланОбмена.DaJetMessaging", "РегистрСведений.НастройкиОбменаРИБ");
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
            settings.ConfigureSelectScripts("ПланОбмена.DaJetMessaging", "РегистрСведений.НастройкиОбменаРИБ");
            List<string> queues = settings.GetIncomingQueueNames();

            foreach (string name in queues)
            {
                Console.WriteLine(name);
            }
        }


        private const string DATABASE_FILE = "C:\\temp\\dajet-vector.db";
        [TestMethod] public void SelectSqliteVector()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));
            
            long vector = db.SelectVector("ЦБ", "Справочник.Номенклатура");

            Console.WriteLine($"Vector = {vector}");

        }
        [TestMethod] public void InsertSqliteVector()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            long vector = db.SelectVector("ЦБ", "Справочник.Номенклатура");

            if (vector > 0)
            {
                return;
            }

            if (db.InsertVector("ЦБ", "Справочник.Номенклатура", 123))
            {
                Console.WriteLine($"Vector inserted.");
            }
        }
        [TestMethod] public void UpdateSqliteVector()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            long vector = db.SelectVector("ЦБ", "Справочник.Номенклатура");

            if (vector > 0)
            {
                if (db.UpdateVector("ЦБ", "Справочник.Номенклатура", ++vector))
                {
                    Console.WriteLine($"Vector updated.");
                }
            }
        }
        [TestMethod] public void DeleteSqliteVector()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            if (db.DeleteVector("ЦБ", "Справочник.Номенклатура"))
            {
                Console.WriteLine($"Vector deleted.");
            }
        }

        [TestMethod] public void InsertVectorCollision()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            if (db.InsertCollision("0095", "Справочник.Валюты", 1234567L, 7654321L))
            {
                Console.WriteLine("Collision inserted.");
            }
        }
        [TestMethod] public void SelectVectorCollision()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            CollisionInfo collision = db.SelectCollision();
            {
                Console.WriteLine($"{collision.Timestamp:yyyy-MM-ddTHH:mm:ss} [{collision.Node}] {collision.Type} ({collision.OldVector}) ({collision.NewVector})");
            }
        }

        [TestMethod] public void BulkInsertSqliteVector()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            Stopwatch watch = new Stopwatch();
            watch.Start();

            int counter = 0;
            
            for (int i = 0; i < 10000; i++)
            {
                if (db.InsertVector("ЦБ", "Справочник." + (i + 1).ToString(), (i + 1)))
                {
                    counter++;
                }
            }

            watch.Stop();

            Console.WriteLine($"Inserted {counter} vectors in {watch.ElapsedMilliseconds} ms.");
        }
        [TestMethod] public void BulkUpdateSqliteVector()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            Stopwatch watch = new Stopwatch();
            watch.Start();

            int counter = 0;

            for (int i = 0; i < 10000; i++)
            {
                if (db.UpdateVector("ЦБ", "Справочник." + (i + 1).ToString(), (i + 100)))
                {
                    counter++;
                }
            }

            watch.Stop();

            Console.WriteLine($"Updated {counter} vectors in {watch.ElapsedMilliseconds} ms.");
        }
        [TestMethod] public void BulkSelectSqliteVector()
        {
            VectorService db = new VectorService(
                Options.Create(new VectorServiceOptions()
                {
                    ConnectionString = DATABASE_FILE
                }));

            Stopwatch watch = new Stopwatch();
            watch.Start();

            int counter = 0;

            for (int i = 0; i < 10000; i++)
            {
                long vector = db.SelectVector("ЦБ", "Справочник." + (i + 1).ToString());
                
                if (vector > 0)
                {
                    counter++;
                }
            }

            watch.Stop();

            Console.WriteLine($"Found {counter} vectors in {watch.ElapsedMilliseconds} ms.");
        }



        [TestMethod] public void RabbitMQ_Produce()
        {
            string queue = "dajet-queue";
            string uri = "amqp://guest:guest@localhost:5672/%2F";

            IOptions<RmqProducerOptions> options = Options.Create(new RmqProducerOptions()
            {
                UseVectorService = true,
                VectorDatabase = "C:\\temp\\dajet-vector.db"
            });

            List<OutgoingMessage> messages = GetTestMessages();

            using (RmqMessageProducer producer = new RmqMessageProducer(uri, queue))
            {
                producer.Configure(options);

                producer.Initialize(ExchangeRoles.Dispatcher);

                foreach (OutgoingMessage message in messages)
                {
                    producer.Publish(message);
                }
            }

            Console.WriteLine($"Produced {messages.Count} messages.");
        }
        [TestMethod] public void RabbitMQ_Consume()
        {
            string uri = "amqp://guest:guest@localhost:5672/%2F";

            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("rmq-test");

            _options = Options.Create(new RmqConsumerOptions()
            {
                Heartbeat = 10,
                UseVectorService = true,
                VectorDatabase = "C:\\temp\\dajet-vector.db",
                Queues = new List<string>() { "dajet-queue" }
            });

            CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            using (RmqMessageConsumer consumer = new RmqMessageConsumer(uri))
            {
                consumer.Configure(_options);

                consumer.Initialize(DatabaseProvider.SQLServer, MS_CONNECTION_STRING, INCOMING_QUEUE_NAME);

                consumer.Consume(stop.Token, FileLogger.Log);
            }
        }
        private List<OutgoingMessage> GetTestMessages()
        {
            OutgoingMessage message;
            List<OutgoingMessage> messages = new List<OutgoingMessage>();

            message = OutgoingMessageDataMapper.Create(10) as OutgoingMessage;
            message.Uuid = Guid.Empty;
            message.MessageNumber = 1;
            message.Sender = "TEST";
            message.Recipients = "TEST";
            message.OperationType = "UPSERT";
            message.DateTimeStamp = DateTime.Now;
            message.MessageType = "Справочник.Тест";
            message.MessageBody = "{ \"msgno\": \"1\" }";
            messages.Add(message);

            message = OutgoingMessageDataMapper.Create(10) as OutgoingMessage;
            message.Uuid = Guid.Empty;
            message.MessageNumber = 2;
            message.Sender = "TEST";
            message.Recipients = "TEST";
            message.OperationType = "UPSERT";
            message.DateTimeStamp = DateTime.Now;
            message.MessageType = "Справочник.Тест";
            message.MessageBody = "{ \"msgno\": \"2\" }";
            messages.Add(message);

            message = OutgoingMessageDataMapper.Create(10) as OutgoingMessage;
            message.Uuid = Guid.Empty;
            message.MessageNumber = 3;
            message.Sender = "TEST";
            message.Recipients = "TEST";
            message.OperationType = "UPSERT";
            message.DateTimeStamp = DateTime.Now;
            message.MessageType = "Справочник.Тест";
            message.MessageBody = "{ \"msgno\": \"3\" }";
            messages.Add(message);

            return messages;
        }
    }
}