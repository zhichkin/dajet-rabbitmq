using DaJet.Data.Mapping;
using DaJet.Data.Messaging;
using DaJet.Json;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Diagnostics;

namespace DaJet.RabbitMQ.Test
{
    [TestClass] public class UsageTests
    {
        private const string OUTGOING_QUEUE_NAME = "РегистрСведений.ИсходящаяОчередь2";
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";

        [TestMethod] public void TestRmqMessageProducer()
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            //TODO: validate database interface version, cash InfoBase and queue metadata

            ApplicationObject queue = infoBase.GetApplicationObjectByName(OUTGOING_QUEUE_NAME);

            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            string routingKey = "dajet-queue"; // TODO: MessageType "Справочник.Номенклатура";
            string uri = "amqp://guest:guest@localhost:5672"; // /%2F /dajet-exchange

            using (IMessageConsumer consumer = new MsMessageConsumer(MS_CONNECTION_STRING, in queue))
            {
                using (RmqMessageProducer producer = new RmqMessageProducer(uri, routingKey))
                {
                    producer.Initialize();
                    producer.AppId = "DaJet.RabbitMQ";

                    int published = producer.Publish(consumer, serializer);

                    Console.WriteLine($"Published {published} messages.");
                }
            }

            watch.Stop();
            Console.WriteLine($"Elapsed in {watch.ElapsedMilliseconds} ms");
        }
    }
}