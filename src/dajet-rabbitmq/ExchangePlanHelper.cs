using DaJet.Data;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using System.Collections.Generic;
using System.Data;

namespace DaJet.RabbitMQ
{
    public sealed class ExchangePlanHelper
    {
        #region "SQL QUERY SCRIPT TEMPLATES"

        private const string MS_THIS_NODE_SELECT_TEMPLATE =
            "SELECT TOP 1 _Code FROM {EXCHANGE_PLAN_TABLE} WHERE _PredefinedID > 0x00000000000000000000000000000000;";

        private const string MS_SENDER_NODES_SELECT_TEMPLATE =
            "SELECT _Code FROM {EXCHANGE_PLAN_TABLE} WHERE _Marked = 0x00 AND _PredefinedID = 0x00000000000000000000000000000000;";

        private const string PG_THIS_NODE_SELECT_TEMPLATE =
            "SELECT CAST(_code AS varchar) FROM {EXCHANGE_PLAN_TABLE} WHERE _predefinedid > E'\\\\x00000000000000000000000000000000' LIMIT 1;";

        private const string PG_SENDER_NODES_SELECT_TEMPLATE =
            "SELECT CAST(_code AS varchar) FROM {EXCHANGE_PLAN_TABLE} WHERE _marked = FALSE AND _predefinedid = E'\\\\x00000000000000000000000000000000';";

        #endregion

        private readonly string _connectionString;
        private readonly DatabaseProvider _provider;

        private InfoBase _infoBase;
        private Dictionary<string, string> _mappings = new Dictionary<string, string>();

        private string THIS_NODE_SELECT_SCRIPT;
        private string SENDER_NODES_SELECT_SCRIPT;

        public ExchangePlanHelper(in InfoBase infoBase, DatabaseProvider provider, string connectionString)
        {
            _infoBase = infoBase;
            _provider = provider;
            _connectionString = connectionString;
        }
        public void ConfigureSelectScripts(in string publicationName, in string registerName)
        {
            _mappings.Clear();

            ApplicationObject settings = _infoBase.GetApplicationObjectByName(registerName);
            ApplicationObject publication = _infoBase.GetApplicationObjectByName(publicationName);

            _mappings.Add("{EXCHANGE_PLAN_TABLE}", publication.TableName);
            _mappings.Add("{INFORMATION_REGISTER_TABLE}", settings.TableName);

            foreach (MetadataProperty property in settings.Properties)
            {
                if (property.Name == "Узел")
                {
                    _mappings.Add("{NODE_REFERENCE}", property.Fields[0].Name);
                }
                else if (property.Name == "ИспользоватьОбменДаннымиRabbitMQ")
                {
                    _mappings.Add("{USE_RABBITMQ}", property.Fields[0].Name);
                }
            }

            if (_provider == DatabaseProvider.SQLServer)
            {
                THIS_NODE_SELECT_SCRIPT = ConfigureScript(MS_THIS_NODE_SELECT_TEMPLATE);
                SENDER_NODES_SELECT_SCRIPT = ConfigureScript(MS_SENDER_NODES_SELECT_TEMPLATE);
            }
            else // PostgreSQL
            {
                THIS_NODE_SELECT_SCRIPT = ConfigureScript(PG_THIS_NODE_SELECT_TEMPLATE);
                SENDER_NODES_SELECT_SCRIPT = ConfigureScript(PG_SENDER_NODES_SELECT_TEMPLATE);
            }
        }
        public List<string> GetIncomingQueueNames()
        {
            List<string> result = new List<string>();

            string recipient = GetThisNodeCode();

            if (string.IsNullOrWhiteSpace(recipient))
            {
                return result;
            }
            else
            {
                recipient = recipient.Trim();
            }

            QueryExecutor executor = new QueryExecutor(_provider, in _connectionString);

            foreach (IDataReader reader in executor.ExecuteReader(SENDER_NODES_SELECT_SCRIPT, 10))
            {
                string sender = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();

                if (string.IsNullOrWhiteSpace(sender))
                {
                    continue;
                }

                result.Add(CreateIncomingQueueName(in sender, in recipient));
            }

            return result;
        }
        private string GetThisNodeCode()
        {
            QueryExecutor executor = new QueryExecutor(_provider, in _connectionString);

            return executor.ExecuteScalar<string>(THIS_NODE_SELECT_SCRIPT, 10);
        }
        private string CreateIncomingQueueName(in string senderCode, in string recipientCode)
        {
            return $"РИБ.{senderCode}.{recipientCode}";
        }
        private string ConfigureScript(in string template)
        {
            string script = template;

            foreach (var item in _mappings)
            {
                script = script.Replace(item.Key, item.Value);
            }

            return script;
        }
    }
}