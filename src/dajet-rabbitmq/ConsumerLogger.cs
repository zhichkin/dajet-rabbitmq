using DaJet.Logging;
using DaJet.Metadata;
using Microsoft.Data.Sqlite;
using System;

namespace DaJet.RabbitMQ
{
    internal sealed class ConsumerLogger
    {
        private readonly string _connectionString;
        private readonly long _logRetention; // seconds

        #region "CONSUMER LOG DATABASE SCHEMA"

        private const string CREATE_LOG_TABLE_SCRIPT =
            "CREATE TABLE IF NOT EXISTS " +
            "log (timestamp INTEGER NOT NULL, sender TEXT NOT NULL, message_type TEXT NOT NULL, message_body TEXT NOT NULL);";

        private const string INSERT_LOG_ENTRY_SCRIPT =
            "INSERT INTO log (timestamp, sender, message_type, message_body) " +
            "VALUES (@timestamp, @sender, @message_type, @message_body);";

        private const string DELETE_LOG_ENTRIES_SCRIPT =
            "DELETE FROM log WHERE timestamp <= @timestamp;";

        //SELECT datetime(timestamp, 'unixepoch') AS "timestamp", sender, message_type, message_body FROM log;

        #endregion

        internal ConsumerLogger(string databaseFile, int retentionInHours)
        {
            _logRetention = retentionInHours * 60L * 60L; // convert hours to seconds

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
                    command.CommandText = CREATE_LOG_TABLE_SCRIPT;

                    _ = command.ExecuteNonQuery();
                }
            }
        }

        internal void Log(string sender, string messageType, string messageBody)
        {
            try
            {
                InsertLogEntry(sender, messageType, messageBody);
            }
            catch (Exception error)
            {
                FileLogger.Log($"[RMQ Consumer Log] {ExceptionHelper.GetErrorText(error)}");
            }
        }
        private long GetUnixDateTimeNow()
        {
            DateTime zero = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return (long)(DateTime.Now - zero).TotalSeconds;
        }
        private void InsertLogEntry(string sender, string messageType, string messageBody)
        {
            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = INSERT_LOG_ENTRY_SCRIPT;

                    command.Parameters.AddWithValue("sender", string.IsNullOrEmpty(sender) ? string.Empty : sender);
                    command.Parameters.AddWithValue("message_type", string.IsNullOrEmpty(messageType) ? string.Empty : messageType);
                    command.Parameters.AddWithValue("message_body", string.IsNullOrEmpty(messageBody) ? string.Empty : messageBody);
                    command.Parameters.AddWithValue("timestamp", GetUnixDateTimeNow());

                    bool success = (command.ExecuteNonQuery() == 1); 
                }
            }
        }

        internal void ClearLog()
        {
            try
            {
                ClearLogEntries();
            }
            catch (Exception error)
            {
                FileLogger.Log($"[RMQ Consumer Log] {ExceptionHelper.GetErrorText(error)}");
            }
        }
        private void ClearLogEntries()
        {
            using (SqliteConnection connection = new SqliteConnection(_connectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = DELETE_LOG_ENTRIES_SCRIPT;

                    command.Parameters.AddWithValue("timestamp", GetUnixDateTimeNow() - _logRetention);

                    int result = command.ExecuteNonQuery();
                }
            }
        }
    }
}