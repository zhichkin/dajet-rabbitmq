using DaJet.Logging;
using DaJet.Metadata;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace DaJet.RabbitMQ
{
    internal enum PublishStatus { New, Ack, Nack }
    internal sealed class PublishTracker
    {
        private long _returned = 0L;
        private long _shutdown = 0L;
        private string _reason = string.Empty;
        private readonly string _connectionString;
        private readonly long _logRetention; // seconds

        private readonly ConcurrentDictionary<ulong, PublishStatus> _tags = new ConcurrentDictionary<ulong, PublishStatus>();

        #region "ERRORS DATABASE SCHEMA"

        private const string CREATE_ERROR_TABLE_SCRIPT =
            "CREATE TABLE IF NOT EXISTS " +
            "errors (timestamp INTEGER NOT NULL, description TEXT NOT NULL);";

        private const string INSERT_ERROR_SCRIPT =
            "INSERT INTO errors (timestamp, description) VALUES (@timestamp, @description);";

        private const string DELETE_ERROR_SCRIPT =
            "DELETE FROM errors WHERE timestamp <= @timestamp;";

        #endregion

        internal PublishTracker(string errorDatabase, int retentionInHours)
        {
            _logRetention = retentionInHours * 60L * 60L; // convert hours to seconds

            _connectionString = new SqliteConnectionStringBuilder()
            {
                DataSource = errorDatabase,
                Mode = SqliteOpenMode.ReadWriteCreate
            }
            .ToString();

            InitializeErrorDatabase();
        }

        internal void Track(ulong deliveryTag)
        {
            _ = _tags.TryAdd(deliveryTag, PublishStatus.New);
        }
        internal void Clear()
        {
            _tags.Clear();
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
        internal void SetReturnedStatus(string reason)
        {
            Interlocked.Increment(ref _returned);

            _reason = reason;
        }
        internal void SetShutdownStatus(string reason)
        {
            Interlocked.Increment(ref _shutdown);

            _reason = reason;
        }
        internal void SetSingleStatus(ulong deliveryTag, PublishStatus status)
        {
            _tags[deliveryTag] = status;
        }
        internal void SetMultipleStatus(ulong deliveryTag, PublishStatus status)
        {
            foreach (var item in _tags)
            {
                if (item.Key <= deliveryTag)
                {
                    _tags[item.Key] = status;
                }
            }
        }

        internal bool IsReturned
        {
            get
            {
                return (Interlocked.Read(ref _returned) > 0);
            }
        }
        internal bool IsShutdown
        {
            get
            {
                return (Interlocked.Read(ref _shutdown) > 0);
            }
        }
        internal string ErrorReason
        {
            get
            {
                if (string.IsNullOrEmpty(_reason))
                {
                    _reason = "Some messages were nacked.";
                }

                return _reason;
            }
        }
        internal bool HasErrors()
        {
            if (IsReturned || IsShutdown)
            {
                return true;
            }

            foreach (var item in _tags)
            {
                if (item.Value != PublishStatus.Ack)
                {
                    return true;
                }
            }

            return false;
        }
        internal void TryLogErrors()
        {
            try
            {
                if (!string.IsNullOrWhiteSpace(ErrorReason))
                {
                    InsertError(ErrorReason);
                }
            }
            catch (Exception error)
            {
                FileLogger.Log($"[Publish Tracker] {ExceptionHelper.GetErrorText(error)}");
            }
        }

        private void InitializeErrorDatabase()
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
                    command.CommandText = CREATE_ERROR_TABLE_SCRIPT;

                    _ = command.ExecuteNonQuery();
                }
            }

            ClearErrors();
        }
        private long GetUnixDateTimeNow()
        {
            DateTime zero = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return (long)(DateTime.Now - zero).TotalSeconds;
        }
        private void InsertError(string description)
        {
            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = INSERT_ERROR_SCRIPT;

                    command.Parameters.AddWithValue("description", description);
                    command.Parameters.AddWithValue("timestamp", GetUnixDateTimeNow());

                    bool success = (command.ExecuteNonQuery() == 1); 
                }
            }
        }
        private void ClearErrors()
        {
            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = DELETE_ERROR_SCRIPT;

                    command.Parameters.AddWithValue("timestamp", GetUnixDateTimeNow() - _logRetention);

                    int result = command.ExecuteNonQuery();
                }
            }
        }
    }
}