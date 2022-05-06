using Microsoft.Data.Sqlite;
using System;

namespace DaJet.Vector
{
    public sealed class SqliteVector
    {
        private const string CREATE_VECTOR_TABLE_SCRIPT = "CREATE TABLE IF NOT EXISTS " +
            "vectors (node TEXT NOT NULL, type TEXT NOT NULL, vector INTEGER NOT NULL, " +
            "PRIMARY KEY (node, type)) WITHOUT ROWID;";
        private const string SELECT_VECTOR_SCRIPT = "SELECT vector FROM vectors WHERE node = @node AND type = @type;";
        private const string INSERT_VECTOR_SCRIPT = "INSERT INTO vectors (node, type, vector) VALUES (@node, @type, @vector);";
        private const string UPDATE_VECTOR_SCRIPT = "UPDATE vectors SET vector = @vector WHERE node = @node AND type = @type;";
        private const string DELETE_VECTOR_SCRIPT = "DELETE FROM vectors WHERE node = @node AND type = @type;";

        private const string CREATE_COLLISION_TABLE_SCRIPT = "CREATE TABLE IF NOT EXISTS " +
            "collisions (node TEXT NOT NULL, type TEXT NOT NULL, vector INTEGER NOT NULL, timestamp INTEGER NOT NULL);";
        private const string INSERT_COLLISION_SCRIPT =
            "INSERT INTO collisions (node, type, vector, timestamp) VALUES (@node, @type, @vector, @timestamp);";
        private const string SELECT_COLLISION_SCRIPT =
            "WITH rowids AS (SELECT rowid FROM collisions ORDER BY rowid ASC LIMIT 1) " +
            "DELETE FROM collisions WHERE rowid IN rowids " +
            "RETURNING rowid, node, type, vector, timestamp;";

        public SqliteVector(string databaseFile)
        {
            DatabaseFile = databaseFile;

            ConnectionString = new SqliteConnectionStringBuilder()
            {
                DataSource = DatabaseFile,
                Mode = SqliteOpenMode.ReadWriteCreate
            }
            .ToString();

            InitializeDatabase();
        }
        public string DatabaseFile { get; private set; }
        public string ConnectionString { get; private set; }
        private void InitializeDatabase()
        {
            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = CREATE_VECTOR_TABLE_SCRIPT;

                    _ = command.ExecuteNonQuery();

                    command.CommandText = CREATE_COLLISION_TABLE_SCRIPT;

                    _ = command.ExecuteNonQuery();
                }
            }
        }
        
        public long SelectVector(string node, string type)
        {
            long vector = 0L; 

            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_VECTOR_SCRIPT;

                    command.Parameters.AddWithValue("node", node);
                    command.Parameters.AddWithValue("type", type);

                    using (SqliteDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            vector = reader.GetInt64(0);
                        }
                        reader.Close();
                    }
                }
            }

            return vector;
        }
        public bool InsertVector(string node, string type, long vector)
        {
            int result;

            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = INSERT_VECTOR_SCRIPT;
                    
                    command.Parameters.AddWithValue("node", node);
                    command.Parameters.AddWithValue("type", type);
                    command.Parameters.AddWithValue("vector", vector);

                    result = command.ExecuteNonQuery();
                }
            }

            return (result == 1);
        }
        public bool UpdateVector(string node, string type, long vector)
        {
            int result;

            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = UPDATE_VECTOR_SCRIPT;

                    command.Parameters.AddWithValue("node", node);
                    command.Parameters.AddWithValue("type", type);
                    command.Parameters.AddWithValue("vector", vector);

                    result = command.ExecuteNonQuery();
                }
            }

            return (result == 1);
        }
        public bool DeleteVector(string node, string type)
        {
            int result;

            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = DELETE_VECTOR_SCRIPT;

                    command.Parameters.AddWithValue("node", node);
                    command.Parameters.AddWithValue("type", type);

                    result = command.ExecuteNonQuery();
                }
            }

            return (result > 0);
        }

        private long GetUnixDateTimeNow()
        {
            DateTime zero = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            
            return (long)(DateTime.Now - zero).TotalSeconds;
        }
        private DateTime GetDateTimeFromUnixTime(long seconds)
        {
            DateTime zero = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return zero.AddSeconds(seconds);
        }
        public bool InsertCollision(string node, string type, long vector)
        {
            int result;

            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = INSERT_COLLISION_SCRIPT;

                    command.Parameters.AddWithValue("node", node);
                    command.Parameters.AddWithValue("type", type);
                    command.Parameters.AddWithValue("vector", vector);
                    command.Parameters.AddWithValue("timestamp", GetUnixDateTimeNow());

                    result = command.ExecuteNonQuery();
                }
            }

            return (result == 1);
        }
        public VectorCollision SelectCollision(string node, string type)
        {
            VectorCollision collision = new VectorCollision();

            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_COLLISION_SCRIPT;

                    using (SqliteDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            collision.RowId = reader.GetInt64(0);
                            collision.Node = reader.GetString(1);
                            collision.Type = reader.GetString(2);
                            collision.Vector = reader.GetInt64(3);
                            collision.Timestamp = GetDateTimeFromUnixTime(reader.GetInt64(4));
                        }
                        reader.Close();
                    }
                }
            }

            return collision;
        }
    }
    public sealed class VectorCollision
    {
        public long RowId { get; set; }
        public string Node { get; set; }
        public string Type { get; set; }
        public long Vector { get; set; }
        public DateTime Timestamp { get; set; }
    }
}