﻿using DaJet.Metadata;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;

namespace DaJet.RabbitMQ
{
    public sealed class EventTracker : IDisposable
    {
        #region "EVENT TRACKER DATABASE SCHEMA"

        private const string CREATE_TRACKER_TABLE_SCRIPT =
            "CREATE TABLE IF NOT EXISTS delivery_tracking_events (" +
            "source TEXT NOT NULL, " +
            "msguid TEXT NOT NULL, " +
            "event_type TEXT NOT NULL, " +
            "event_node TEXT NOT NULL, " +
            "event_time INTEGER NOT NULL, " +
            "event_data TEXT NOT NULL, " +
            "PRIMARY KEY (source, msguid, event_type, event_node)) WITHOUT ROWID;";

        private const string UPSERT_TRACKER_EVENT_SCRIPT =
            "INSERT INTO delivery_tracking_events (" +
            "source, msguid, event_type, event_node, event_time, event_data) " +
            "VALUES (" +
            "@source, @msguid, @event_type, @event_node, @event_time, @event_data) " +
            "ON CONFLICT (source, msguid, event_type, event_node) " +
            "DO UPDATE SET " +
            "event_time = excluded.event_time, " +
            "event_data = excluded.event_data;";

        private const string SELECT_TRACKER_EVENT_SCRIPT =
            "WITH filter AS " +
            "(SELECT source, msguid, event_type, event_node FROM delivery_tracking_events LIMIT 1000) " +
            "DELETE FROM delivery_tracking_events " +
            "WHERE (source, msguid, event_type, event_node) IN filter " +
            "RETURNING source, msguid, event_type, event_node, event_time, event_data;";

        #endregion

        private readonly DateTime UNIX_ZERO_TIME = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly ConcurrentDictionary<ulong, TrackerEvent> _tags = new ConcurrentDictionary<ulong, TrackerEvent>();
        private readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions()
        {
            WriteIndented = false,
            Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
        };

        private string _connectionString;
        public EventTracker()
        {
            Configure();
        }
        private void Configure()
        {
            Assembly asm = Assembly.GetExecutingAssembly();
            string catalogPath = Path.GetDirectoryName(asm.Location);
            string databaseFile = Path.Combine(catalogPath, "dajet-monitor.db");

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
            return (long)(DateTime.UtcNow - UNIX_ZERO_TIME).TotalSeconds;
        }
        private long GetUnixDateTime(DateTime dateTime)
        {
            return (long)(dateTime - UNIX_ZERO_TIME).TotalSeconds;
        }
        private DateTime GetDateTimeFromUnixTime(long seconds)
        {
            return UNIX_ZERO_TIME.AddSeconds(seconds);
        }

        public void RegisterEvent(TrackerEvent @event)
        {

            string eventData = string.Empty;

            try
            {
                if (@event.EventData != null)
                {
                    eventData = JsonSerializer.Serialize(@event.EventData, @event.EventData.GetType(), _serializerOptions);
                }
            }
            catch (Exception error)
            {
                eventData = ExceptionHelper.GetErrorText(error);
            }

            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = UPSERT_TRACKER_EVENT_SCRIPT;

                    command.Parameters.AddWithValue("source", @event.Source);
                    command.Parameters.AddWithValue("msguid", @event.MsgUid);
                    command.Parameters.AddWithValue("event_type", @event.EventType);
                    command.Parameters.AddWithValue("event_node", @event.EventNode);
                    command.Parameters.AddWithValue("event_time", GetUnixDateTime(@event.EventTime));
                    command.Parameters.AddWithValue("event_data", eventData);

                    int recordsAffected = command.ExecuteNonQuery();
                }

                _ = _tags.TryAdd(@event.DeliveryTag, @event);
            }
        }
        public IEnumerable<TrackerEvent> SelectTrackerEvents()
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
                                @event.Source = reader.GetString(0);
                                @event.MsgUid = reader.GetString(1);
                                @event.EventType = reader.GetString(2);
                                @event.EventNode = reader.GetString(3);
                                @event.EventTime = GetDateTimeFromUnixTime(reader.GetInt64(4));
                                @event.EventData = reader.GetString(5);

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
            RegisterDeliveryStatus(_tags[deliveryTag], status);
        }
        private void SetMultipleStatus(ulong deliveryTag, PublishStatus status)
        {
            foreach (var item in _tags)
            {
                if (item.Key <= deliveryTag)
                {
                    RegisterDeliveryStatus(_tags[item.Key], status);
                }
            }
        }
        private void RegisterDeliveryStatus(TrackerEvent @event, PublishStatus status)
        {
            string eventType = TrackerEventType.UNDEFINED;

            if (status == PublishStatus.Ack)
            {
                eventType = TrackerEventType.DBRMQ_ACK;
            }
            else if (status == PublishStatus.Nack)
            {
                eventType = TrackerEventType.DBRMQ_NACK;
            }

            RegisterEvent(new TrackerEvent()
            {
                DeliveryTag = @event.DeliveryTag,
                Source = @event.Source,
                MsgUid = @event.MsgUid,
                EventNode = @event.EventNode,
                EventType = eventType
            });
        }
        internal void SetReturnedStatus(string appId, string messageId, string reason)
        {
            RegisterEvent(new TrackerEvent()
            {
                EventType = TrackerEventType.DBRMQ_RETURN,
                Source = appId,
                MsgUid = messageId,
                EventData = new ReturnEvent() { Reason = reason }
            });
        }
        internal void SetShutdownStatus(string reason)
        {
            //TODO: ???
            //if (!string.IsNullOrEmpty(reason))
            //{
            //    RegisterEvent(new TrackerEvent()
            //    {
            //        EventType = $"DBRMQ_SHUTDOWN",
            //        EventData = new ShutdownEvent() { Reason = reason }
            //    });
            //}

            Dispose();
        }
    }
}