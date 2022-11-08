using DaJet.Data.Mapping;
using DaJet.Data.Messaging;
using DaJet.Data.Messaging.V10;
using DaJet.Json;
using DaJet.Logging;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Timers;

namespace DaJet.RabbitMQ.Test
{
    [TestClass] public class UsageTests
    {
        private const bool USE_DELIVERY_TRACKING = true;
        private const string DELIVERY_TRACKING_QUEUE = "dajet-agent-monitor";
        private const string INCOMING_QUEUE_NAME = "���������������.���������������10";
        private const string OUTGOING_QUEUE_NAME = "���������������.����������������10";
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True;Encrypt=False;";
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
                Console.WriteLine($"������ ���������� \"{OUTGOING_QUEUE_NAME}\" �� ������."); return;
            }
            Console.WriteLine($"{queue.Name} [{queue.TableName}]");

            int version = GetOutgoingDataContractVersion(in queue);
            if (version < 1) { return; }
            Console.WriteLine($"������ ��������� �������: {version}");

            if (!ConfigureOutgoingQueue(version, in queue))
            {
                return;
            }
            Console.WriteLine();

            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            string routingKey = "����������.������������������";
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
                Console.WriteLine($"�� ������� ���������� ������ ��������� ������.");
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

            Console.WriteLine($"��������� ������� ��������� �������.");

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
            settings.ConfigureSelectScripts("����������.DaJetMessaging", "test.test");

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
            settings.ConfigureSelectScripts("����������.DaJetMessaging", "���������������.������������������");
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
            settings.ConfigureSelectScripts("����������.DaJetMessaging", "���������������.������������������");
            List<string> queues = settings.GetIncomingQueueNames();

            foreach (string name in queues)
            {
                Console.WriteLine(name);
            }
        }



        private const string DATABASE_FILE = "C:\\temp\\dajet-vector.db";
        private const string ERROR_LOG_FILE = "C:\\temp\\producer-errors.db";
        [TestMethod] public void RabbitMQ_Produce()
        {
            string queue = "dajet-queue"; // "dajet-queue_not_found"
            string uri = "amqp://guest:guest@localhost:5672/%2F";

            IOptions<RmqProducerOptions> options = Options.Create(new RmqProducerOptions()
            {
                UseVectorService = false,
                VectorDatabase = DATABASE_FILE,
                ErrorLogDatabase = ERROR_LOG_FILE,
                ErrorLogRetention = 1 // one hour = 3600 seconds
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

                producer.Confirm();
            }

            Console.WriteLine($"Produced {messages.Count} messages.");
        }
        private List<OutgoingMessage> GetTestMessages()
        {
            OutgoingMessage message;

            List<OutgoingMessage> messages = new List<OutgoingMessage>();

            for (int i = 0; i < 10; i++)
            {
                message = OutgoingMessageDataMapper.Create(10) as OutgoingMessage;
                message.Uuid = Guid.Empty;
                message.MessageNumber = (i + 1);
                message.Sender = "TEST";
                message.Recipients = "N001,N002,N003"; // "����";
                message.OperationType = "UPSERT";
                message.DateTimeStamp = DateTime.Now;
                message.MessageType = (i % 2 == 0) ? "����������.����" : "���������������.����";
                message.MessageBody = $"{{ \"������\": \"{(i + 1)}\" }}";
                messages.Add(message);
            }

            return messages;
        }


        [TestMethod] public void MS_DeliveryTracking_ConfigureDatabase()
        {
            DeliveryTracker tracker = new MsDeliveryTracker(MS_CONNECTION_STRING);

            tracker.ConfigureDatabase();

            Console.WriteLine("SUCCESS");
        }
        [TestMethod] public void MS_DeliveryTracker_RegisterEvents()
        {
            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("delivery-tracking");

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
                Console.WriteLine($"������ ���������� \"{OUTGOING_QUEUE_NAME}\" �� ������.");
                return;
            }

            string uri = "amqp://guest:guest@localhost:5672/%2F";

            IOptions<RmqProducerOptions> options = Options.Create(new RmqProducerOptions()
            {
                ThisNode = "MAIN",
                Provider = DatabaseProvider.SQLServer,
                ConnectionString = MS_CONNECTION_STRING,
                UseDeliveryTracking = USE_DELIVERY_TRACKING
            });

            Stopwatch watch = new Stopwatch();

            watch.Start();

            using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in queue))
            {
                using (RmqMessageProducer producer = new RmqMessageProducer(uri, "dajet-queue")) // dajet-queue-not-found
                {
                    producer.Configure(options);

                    producer.Initialize(ExchangeRoles.Dispatcher);

                    int published = producer.Publish(consumer);

                    Console.WriteLine($"Produced {published} messages.");
                }
            }

            watch.Stop();

            Console.WriteLine($"USE DELIVERY TRACKING [{USE_DELIVERY_TRACKING}] = {watch.ElapsedMilliseconds} ms");
        }
        [TestMethod] public void MS_DeliveryTracker_ProcessEvents()
        {
            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("delivery-tracking");

            string uri = "amqp://guest:guest@localhost:5672/%2F";

            IOptions<RmqProducerOptions> options = Options.Create(new RmqProducerOptions()
            {
                ThisNode = "MAIN",
                Provider = DatabaseProvider.SQLServer,
                ConnectionString = MS_CONNECTION_STRING,
                UseDeliveryTracking = USE_DELIVERY_TRACKING
            });

            CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            int published;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            using (RmqMessageProducer producer = new RmqMessageProducer(uri, DELIVERY_TRACKING_QUEUE))
            {
                producer.Configure(options);
                producer.Initialize();
                published = producer.PublishDeliveryTrackingEvents(stop.Token);
            }

            watch.Stop();
            Console.WriteLine($"Published {published} events in {watch.ElapsedMilliseconds} ms");
        }
        [TestMethod] public void MS_DeliveryTracker_Consume()
        {
            string uri = "amqp://guest:guest@localhost:5672/%2F";

            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("delivery-tracking");

            _options = Options.Create(new RmqConsumerOptions()
            {
                ThisNode = "N001",
                Heartbeat = 10,
                UseDeliveryTracking = false, //USE_DELIVERY_TRACKING,
                Queues = new List<string>() { "dajet-queue" }
            });

            CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            using (RmqMessageConsumer consumer = new RmqMessageConsumer(uri))
            {
                consumer.Configure(_options);

                consumer.Initialize(DatabaseProvider.SQLServer, MS_CONNECTION_STRING, INCOMING_QUEUE_NAME);

                consumer.Consume(stop.Token, FileLogger.Log);
            }
        }



        [TestMethod] public void PG_DeliveryTracking_ConfigureDatabase()
        {
            DeliveryTracker tracker = new PgDeliveryTracker(PG_CONNECTION_STRING);

            tracker.ConfigureDatabase();

            Console.WriteLine("SUCCESS");
        }
        [TestMethod] public void PG_DeliveryTracker_RegisterEvents()
        {
            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("delivery-tracking");

            if (!new MetadataService()
                .UseConnectionString(PG_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName(OUTGOING_QUEUE_NAME);
            if (queue == null)
            {
                Console.WriteLine($"������ ���������� \"{OUTGOING_QUEUE_NAME}\" �� ������.");
                return;
            }

            string uri = "amqp://guest:guest@localhost:5672/%2F";

            IOptions<RmqProducerOptions> options = Options.Create(new RmqProducerOptions()
            {
                ThisNode = "MAIN",
                Provider = DatabaseProvider.PostgreSQL,
                ConnectionString = PG_CONNECTION_STRING,
                UseDeliveryTracking = USE_DELIVERY_TRACKING
            });

            Stopwatch watch = new Stopwatch();

            watch.Start();

            using (IMessageConsumer consumer = new PgMessageConsumer(PG_CONNECTION_STRING, in queue))
            {
                using (RmqMessageProducer producer = new RmqMessageProducer(uri, "dajet-queue")) // dajet-queue-not-found
                {
                    producer.Configure(options);

                    producer.Initialize(ExchangeRoles.Dispatcher);

                    int published = producer.Publish(consumer);

                    Console.WriteLine($"Produced {published} messages.");
                }
            }

            watch.Stop();

            Console.WriteLine($"USE DELIVERY TRACKING [{USE_DELIVERY_TRACKING}] = {watch.ElapsedMilliseconds} ms");
        }
        [TestMethod] public void PG_DeliveryTracker_ProcessEvents()
        {
            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("delivery-tracking");

            string uri = "amqp://guest:guest@localhost:5672/%2F";

            IOptions<RmqProducerOptions> options = Options.Create(new RmqProducerOptions()
            {
                ThisNode = "MAIN",
                Provider = DatabaseProvider.PostgreSQL,
                ConnectionString = PG_CONNECTION_STRING,
                UseDeliveryTracking = USE_DELIVERY_TRACKING
            });

            CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            int published;

            Stopwatch watch = new Stopwatch();
            watch.Start();

            using (RmqMessageProducer producer = new RmqMessageProducer(uri, DELIVERY_TRACKING_QUEUE))
            {
                producer.Configure(options);
                producer.Initialize();
                published = producer.PublishDeliveryTrackingEvents(stop.Token);
            }

            watch.Stop();
            Console.WriteLine($"Published {published} events in {watch.ElapsedMilliseconds} ms");
        }
        [TestMethod] public void PG_DeliveryTracker_Consume()
        {
            string uri = "amqp://guest:guest@localhost:5672/%2F";

            FileLogger.UseCatalog("C:\\temp");
            FileLogger.UseFileName("delivery-tracking");

            _options = Options.Create(new RmqConsumerOptions()
            {
                ThisNode = "N001",
                Heartbeat = 10,
                UseDeliveryTracking = USE_DELIVERY_TRACKING,
                Queues = new List<string>() { "dajet-queue" }
            });

            CancellationTokenSource stop = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            using (RmqMessageConsumer consumer = new RmqMessageConsumer(uri))
            {
                consumer.Configure(_options);

                consumer.Initialize(DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING, INCOMING_QUEUE_NAME);

                consumer.Consume(stop.Token, FileLogger.Log);
            }
        }
    }
}