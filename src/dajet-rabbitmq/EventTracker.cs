using Microsoft.Data.Sqlite;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace DaJet.RabbitMQ
{
    internal sealed class EventTracker : IDisposable
    {
        #region "EVENT TRACKER DATABASE SCHEMA"

        private const string CREATE_TRACKER_TABLE_SCRIPT =
            "CREATE TABLE IF NOT EXISTS tracker_events (" +
            "event_time INTEGER NOT NULL, " +
            "event_type TEXT NOT NULL, " +
            "event_data TEXT NOT NULL, " +
            "source TEXT NOT NULL, target TEXT NOT NULL, " +
            "msg_id TEXT NOT NULL, msg_type TEXT NOT NULL, msg_body TEXT NOT NULL" +
            ");";

        private const string INSERT_TRACKER_EVENT_SCRIPT =
            "INSERT INTO tracker_events (" +
            "event_time, event_type, event_data, " +
            "source, target, msg_id, msg_type, msg_body) " +
            "VALUES (" +
            "@event_time, @event_type, @event_data, " +
            "@source, @target, @msg_id, @msg_type, @msg_body) " +
            "RETURNING rowid;";

        private const string UPDATE_TRACKER_EVENT_SCRIPT =
            "UPDATE tracker_events " +
            "SET event_data = @event_data " +
            "WHERE rowid = @rowid;";

        private const string SELECT_TRACKER_EVENT_SCRIPT =
            "WITH rowids AS (SELECT rowid FROM tracker_events ORDER BY rowid ASC LIMIT 1000) " +
            "DELETE FROM tracker_events WHERE rowid IN rowids " +
            "RETURNING " +
            "event_time, event_type, event_data, " +
            "source, target, msg_id, msg_type, msg_body" +
            ";";

        #endregion

        private readonly DateTime UNIX_ZERO_TIME = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly ConcurrentDictionary<ulong, TrackerEvent> _tags = new ConcurrentDictionary<ulong, TrackerEvent>();

        private string _connectionString;
        internal EventTracker()
        {
            Configure();
        }
        private void Configure()
        {
            Assembly asm = Assembly.GetExecutingAssembly();
            string catalogPath = Path.GetDirectoryName(asm.Location);
            string databaseFile = Path.Combine(catalogPath, "dajet-tracker.db");

            _connectionString = new SqliteConnectionStringBuilder()
            {
                DataSource = databaseFile,
                Mode = SqliteOpenMode.ReadWriteCreate
            }
            .ToString();

            InitializeDatabase();
        }
        private void InitializeDatabase()
        {
            if (string.IsNullOrWhiteSpace(_connectionString))
            {
                return;
            }

            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = CREATE_TRACKER_TABLE_SCRIPT;

                    _ = command.ExecuteNonQuery();
                }
            }
        }
        public void Dispose()
        {
            _tags.Clear();
        }
        
        private long GetUnixDateTimeNow()
        {
            return (long)(DateTime.Now - UNIX_ZERO_TIME).TotalSeconds;
        }
        private long GetUnixDateTime(DateTime dateTime)
        {
            return (long)(dateTime - UNIX_ZERO_TIME).TotalSeconds;
        }
        private DateTime GetDateTimeFromUnixTime(long seconds)
        {
            return UNIX_ZERO_TIME.AddSeconds(seconds);
        }

        internal void Track(TrackerEvent @event)
        {
            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = INSERT_TRACKER_EVENT_SCRIPT;

                    command.Parameters.AddWithValue("event_time", GetUnixDateTime(@event.EventTime));
                    command.Parameters.AddWithValue("event_type", @event.EventType);
                    command.Parameters.AddWithValue("event_data", @event.EventData);
                    command.Parameters.AddWithValue("source", @event.Source);
                    command.Parameters.AddWithValue("target", @event.Target);
                    command.Parameters.AddWithValue("msg_id", @event.MessageId);
                    command.Parameters.AddWithValue("msg_type", @event.MessageType);
                    command.Parameters.AddWithValue("msg_body", @event.MessageBody);

                    using (SqliteDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            @event.RowId = reader.GetInt64(0); // rowid
                        }
                        reader.Close();
                    }
                }
                _ = _tags.TryAdd(@event.DeliveryTag, @event);
            }
        }
        internal void SetEventData(TrackerEvent @event)
        {
            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = UPDATE_TRACKER_EVENT_SCRIPT;

                    command.Parameters.AddWithValue("rowid", @event.RowId);
                    command.Parameters.AddWithValue("event_data", @event.EventData);

                    bool success = (command.ExecuteNonQuery() == 1);
                }
            }
        }
        internal IEnumerable<TrackerEvent> SelectTrackerEvents()
        {
            TrackerEvent @event = new TrackerEvent();

            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using(SqliteTransaction transaction = connection.BeginTransaction())
                {
                    using (SqliteCommand command = connection.CreateCommand())
                    {
                        command.CommandText = SELECT_TRACKER_EVENT_SCRIPT;

                        using (SqliteDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                @event.EventTime = GetDateTimeFromUnixTime(reader.GetInt64(0));
                                @event.EventType = reader.GetString(1);
                                @event.EventData = reader.GetString(2);
                                @event.Source = reader.GetString(3);
                                @event.Target = reader.GetString(4);
                                @event.MessageId = reader.GetString(5);
                                @event.MessageType = reader.GetString(6);
                                @event.MessageBody = reader.GetString(7);

                                yield return @event;
                            }
                            reader.Close();
                        }
                    }
                    transaction.Commit();
                }
            }
        }

        internal void SetAckStatus(ulong deliveryTag, bool multiple)
        {
            if (multiple)
            {
                SetMultipleStatus(deliveryTag, PublishStatus.Ack);
            }
            else
            {
                SetSingleStatus(deliveryTag, PublishStatus.Ack);
            }
        }
        internal void SetNackStatus(ulong deliveryTag, bool multiple)
        {
            if (multiple)
            {
                SetMultipleStatus(deliveryTag, PublishStatus.Nack);
            }
            else
            {
                SetSingleStatus(deliveryTag, PublishStatus.Nack);
            }
        }
        private void SetSingleStatus(ulong deliveryTag, PublishStatus status)
        {
            TrackerEvent @event = _tags[deliveryTag];

            @event.EventData = status.ToString();

            SetEventData(@event);
        }
        private void SetMultipleStatus(ulong deliveryTag, PublishStatus status)
        {
            TrackerEvent @event;

            foreach (var item in _tags)
            {
                if (item.Key <= deliveryTag)
                {
                    @event = _tags[item.Key];

                    @event.EventData = status.ToString();

                    SetEventData(@event);
                }
            }
        }
        internal void SetReturnedStatus(string reason)
        {
            // TODO
        }
        internal void SetShutdownStatus(string reason)
        {
            Dispose();
        }
    }
}