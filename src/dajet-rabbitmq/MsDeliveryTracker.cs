using DaJet.Data;
using DaJet.Metadata;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;

namespace DaJet.RabbitMQ
{
    public sealed class MsDeliveryTracker : DeliveryTracker
    {
        #region "SQL COMMANDS"

        private const string TABLE_EXISTS_COMMAND =
            "SELECT 1 FROM sys.tables WHERE name = 'delivery_tracking_events';";

        private const string CREATE_TABLE_COMMAND =
            "CREATE TABLE delivery_tracking_events (" +
            "msguid uniqueidentifier NOT NULL, " +
            "source nvarchar(10) NOT NULL, " +
            "event_type nvarchar(16) NOT NULL, " +
            "event_node nvarchar(10) NOT NULL, " +
            "event_time datetime2 NOT NULL, " +
            "event_data nvarchar(max) NOT NULL);";

        private const string CREATE_INDEX_COMMAND =
            "CREATE UNIQUE CLUSTERED INDEX ix_delivery_tracking_events " +
            "ON delivery_tracking_events (msguid, source, event_type, event_node);";

        private const string UPDATE_COMMAND =
            "UPDATE delivery_tracking_events WITH (UPDLOCK, SERIALIZABLE) " +
            "SET event_time = @event_time, event_data = @event_data " +
            "WHERE msguid = @msguid AND source = @source " +
            "AND event_type = @event_type AND event_node = @event_node;";

        private const string INSERT_COMMAND =
            "INSERT delivery_tracking_events " +
            "(msguid, source, event_type, event_node, event_time, event_data) " +
            "VALUES " +
            "(@msguid, @source, @event_type, @event_node, @event_time, @event_data);";

        private const string SELECT_COMMAND =
            "WITH cte AS (SELECT TOP 1000 " +
            "msguid, source, event_type, event_node, event_time, event_data " +
            "FROM delivery_tracking_events WITH (ROWLOCK, READPAST) " +
            "ORDER BY msguid, source, event_type, event_node) " +
            "DELETE cte OUTPUT " +
            "deleted.msguid, deleted.source, deleted.event_type, " +
            "deleted.event_node, deleted.event_time, deleted.event_data;";

        #endregion

        private readonly QueryExecutor _executor;
        private readonly string _connectionString;
        public MsDeliveryTracker(string connectionString) : base()
        {
            _connectionString = connectionString;
            _executor = new QueryExecutor(DatabaseProvider.SQLServer, in _connectionString);
        }
        public override void ConfigureDatabase()
        {
            if (_executor.ExecuteScalar<int>(TABLE_EXISTS_COMMAND, 10) == 1)
            {
                return;
            }

            List<string> scripts = new List<string>()
            {
                CREATE_TABLE_COMMAND,
                CREATE_INDEX_COMMAND
            };
            
            _executor.TxExecuteNonQuery(in scripts, 10);
        }
        public override void RegisterEvent(DeliveryEvent @event)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                SqlCommand command = connection.CreateCommand();
                SqlTransaction transaction = connection.BeginTransaction();

                command.Connection = connection;
                command.Transaction = transaction;

                command.Parameters.AddWithValue("msguid", @event.MsgUid);
                command.Parameters.AddWithValue("source", @event.Source);
                command.Parameters.AddWithValue("event_type", @event.EventType);
                command.Parameters.AddWithValue("event_node", @event.EventNode);
                command.Parameters.AddWithValue("event_time", @event.EventTime);
                command.Parameters.AddWithValue("event_data", @event.SerializeEventDataToJson());

                try
                {
                    command.CommandText = UPDATE_COMMAND;

                    int recordsAffected = command.ExecuteNonQuery();

                    if (recordsAffected == 0)
                    {
                        command.CommandText = INSERT_COMMAND;
                        _ = command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }
                catch (Exception error)
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch
                    {
                        // do nothing
                    }
                    throw error;
                }
            }
        }
        public override void ProcessEvents(IDeliveryEventProcessor processor)
        {
            DeliveryEvent @event = new DeliveryEvent();

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (SqlTransaction transaction = connection.BeginTransaction())
                {
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandText = SELECT_COMMAND;

                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                @event.MsgUid = reader.GetGuid(0);
                                @event.Source = reader.GetString(1);
                                @event.EventType = reader.GetString(2);
                                @event.EventNode = reader.GetString(3);
                                @event.EventTime = reader.GetDateTime(4);
                                @event.EventData = reader.GetString(5);

                                processor.Process(@event);
                            }
                            reader.Close();
                        }
                    }
                    transaction.Commit();
                }
            }
        }
    }
}