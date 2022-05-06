using DaJet.Data.Mapping;
using DaJet.Data.Messaging;
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
        private const string INCOMING_QUEUE_NAME = "РегистрСведений.ВходящаяОчередь11"; // "РегистрСведений.ТестоваяВходящаяОчередь";
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
            SqliteVector db = new SqliteVector(DATABASE_FILE);
            
            long vector = db.SelectVector("ЦБ", "Справочник.Номенклатура");

            Console.WriteLine($"Vector = {vector}");

        }
        [TestMethod] public void InsertSqliteVector()
        {
            SqliteVector db = new SqliteVector(DATABASE_FILE);

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
            SqliteVector db = new SqliteVector(DATABASE_FILE);

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
            SqliteVector db = new SqliteVector(DATABASE_FILE);

            if (db.DeleteVector("ЦБ", "Справочник.Номенклатура"))
            {
                Console.WriteLine($"Vector deleted.");
            }
        }

        [TestMethod] public void InsertVectorCollision()
        {
            SqliteVector db = new SqliteVector(DATABASE_FILE);

            if (db.InsertCollision("0095", "Справочник.Валюты", 1234567L))
            {
                Console.WriteLine("Collision inserted.");
            }
        }
        [TestMethod] public void SelectVectorCollision()
        {
            SqliteVector db = new SqliteVector(DATABASE_FILE);

            VectorCollision collision = db.SelectCollision("0095", "Справочник.Валюты");
            {
                Console.WriteLine($"{collision.Timestamp:yyyy-MM-ddTHH:mm:ss} [{collision.Node}] {collision.Type} {collision.Vector}");
            }
        }

        [TestMethod] public void BulkInsertSqliteVector()
        {
            SqliteVector db = new SqliteVector(DATABASE_FILE);

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
            SqliteVector db = new SqliteVector(DATABASE_FILE);

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
            SqliteVector db = new SqliteVector(DATABASE_FILE);

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
    }
}