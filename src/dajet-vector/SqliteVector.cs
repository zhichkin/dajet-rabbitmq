using Microsoft.Data.Sqlite;

namespace DaJet.Vector
{
    public sealed class SqliteVector
    {
        private const string CREATE_TABLE_SCRIPT = "CREATE TABLE IF NOT EXISTS " +
            "vectors (node TEXT NOT NULL, type TEXT NOT NULL, vector INTEGER NOT NULL);";

        private const string SELECT_VECTOR_SCRIPT = "SELECT vector FROM vectors WHERE node = @node AND type = @type;";
        private const string INSERT_VECTOR_SCRIPT = "INSERT INTO vectors (node, type, vector) VALUES (@node, @type, @vector);";
        private const string UPDATE_VECTOR_SCRIPT = "UPDATE vectors SET vector = @vector WHERE node = @node AND type = @type;";
        private const string DELETE_VECTOR_SCRIPT = "DELETE FROM vectors WHERE node = @node AND type = @type;";
        public SqliteVector(string databaseFile)
        {
            DatabaseFile = databaseFile;

            InitializeDatabase();
        }
        public string DatabaseFile { get; private set; }
        private string ConnectionString
        {
            get
            {
                return new SqliteConnectionStringBuilder()
                {
                    DataSource = DatabaseFile,
                    Mode = SqliteOpenMode.ReadWriteCreate
                }
                .ToString();
            }
        }
        private void InitializeDatabase()
        {
            using (SqliteConnection connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (SqliteCommand command = connection.CreateCommand())
                {
                    command.CommandText = CREATE_TABLE_SCRIPT;

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
    }
}