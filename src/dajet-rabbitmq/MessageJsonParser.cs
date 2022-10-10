using System;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace DaJet.RabbitMQ
{
    internal static class MessageJsonParser //TODO: move to DaJet.Json package
    {
        private static readonly string DOCUMENT_TYPE = "Документ";
        private static readonly string REFERENCE_TYPE = "Справочник";

        private static readonly byte[] REF_PROPERTY = Encoding.UTF8.GetBytes("Ref");
        private static readonly byte[] VALUE_PROPERTY = Encoding.UTF8.GetBytes("#value");
        private static readonly byte[] ССЫЛКА_PROPERTY = Encoding.UTF8.GetBytes("Ссылка");

        private static CultureInfo GetCulture()
        {
            try
            {
                return CultureInfo.GetCultureInfo("ru-RU");
            }
            catch (CultureNotFoundException)
            {
                return CultureInfo.InvariantCulture;
            }
        }
        private static bool IsReferenceType(string messageType)
        {
            //CompareInfo comparator = GetCulture().CompareInfo;
            
            //bool result =
            //    comparator.IsPrefix(messageType, DOCUMENT_TYPE, CompareOptions.Ordinal) ||
            //    comparator.IsPrefix(messageType, REFERENCE_TYPE, CompareOptions.Ordinal);

            return messageType.StartsWith(DOCUMENT_TYPE, StringComparison.Ordinal)
                || messageType.StartsWith(REFERENCE_TYPE, StringComparison.Ordinal);
        }
        internal static string GetReferenceValue(string messageType, ReadOnlyMemory<byte> messageBody)
        {
            if (!IsReferenceType(messageType))
            {
                return null;
            }

            Utf8JsonReader reader = new Utf8JsonReader(messageBody.Span);

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals(VALUE_PROPERTY)) // "#value"
                    {
                        while (reader.Read())
                        {
                            if (reader.TokenType == JsonTokenType.PropertyName)
                            {
                                if (reader.ValueTextEquals(REF_PROPERTY)) // "Ref"
                                {
                                    if (reader.Read() && reader.TokenType == JsonTokenType.String)
                                    {
                                        return reader.GetString();
                                    }
                                }
                            }
                        }
                    }
                    else if (reader.ValueTextEquals(ССЫЛКА_PROPERTY))
                    {
                        if (reader.Read() && reader.TokenType == JsonTokenType.String)
                        {
                            return reader.GetString();
                        }
                    }    
                }
            }

            return null;
        }
    }
}