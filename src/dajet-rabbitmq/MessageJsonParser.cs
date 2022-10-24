using DaJet.Metadata;
using System;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace DaJet.RabbitMQ
{
    internal static class MessageJsonParser
    {
        #region "CONSTANTS"

        private static readonly string DOCUMENT_TYPE = "Документ";
        private static readonly string REFERENCE_TYPE = "Справочник";
        private static readonly string INFO_REGISTER_TYPE = "РегистрСведений";
        private static readonly string ACCUM_REGISTER_TYPE = "РегистрНакопления";

        private static readonly byte[] REF_PROPERTY = Encoding.UTF8.GetBytes("Ref");
        private static readonly byte[] VALUE_PROPERTY = Encoding.UTF8.GetBytes("#value");
        private static readonly byte[] ССЫЛКА_PROPERTY = Encoding.UTF8.GetBytes("Ссылка");
        private static readonly byte[] FILTER_PROPERTY = Encoding.UTF8.GetBytes("Filter");
        private static readonly byte[] DELETE_PROPERTY = Encoding.UTF8.GetBytes("delete");

        #endregion

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
        private static bool IsRegisterType(string messageType)
        {
            return messageType.StartsWith(INFO_REGISTER_TYPE, StringComparison.Ordinal)
                || messageType.StartsWith(ACCUM_REGISTER_TYPE, StringComparison.Ordinal);
        }

        internal static string ExtractEntityKey(string messageType, ReadOnlyMemory<byte> messageBody)
        {
            try
            {
                return TryExtractEntityKey(messageType, messageBody);
            }
            catch (Exception error)
            {
                return ExceptionHelper.GetErrorText(error);
            }
        }
        private static string TryExtractEntityKey(string messageType, ReadOnlyMemory<byte> messageBody)
        {
            if (IsReferenceType(messageType))
            {
                return GetReferenceValue(in messageBody);
            }
            else if (IsRegisterType(messageType))
            {
                return GetRegisterFilterValue(in messageBody);
            }
            return string.Empty;
        }
        internal static string GetReferenceValue(in ReadOnlyMemory<byte> messageBody)
        {
            Utf8JsonReader reader = new Utf8JsonReader(messageBody.Span);

            while (reader.Read())
            {
                if (reader.CurrentDepth != 1)
                {
                    continue;
                }

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
                    else if (reader.ValueTextEquals(ССЫЛКА_PROPERTY)) // "Ссылка"
                    {
                        if (reader.Read() && reader.TokenType == JsonTokenType.String)
                        {
                            return reader.GetString();
                        }
                    }    
                }
            }

            return string.Empty;
        }
        internal static string GetRegisterFilterValue(in ReadOnlyMemory<byte> messageBody)
        {
            Utf8JsonReader reader = new Utf8JsonReader(messageBody.Span);

            while (reader.Read())
            {
                if (reader.CurrentDepth != 1)
                {
                    continue;
                }

                if (reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals(VALUE_PROPERTY)) // "#value"
                    {
                        while (reader.Read())
                        {
                            if (reader.TokenType == JsonTokenType.PropertyName)
                            {
                                if (reader.ValueTextEquals(FILTER_PROPERTY)) // "Filter"
                                {
                                    if (reader.Read() && reader.TokenType == JsonTokenType.StartArray)
                                    {
                                        return GetFilterValue(ref reader, in messageBody, JsonTokenType.EndArray);
                                    }
                                }
                            }
                        }
                    }
                    else if (reader.ValueTextEquals(DELETE_PROPERTY)) // "delete"
                    {
                        if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                        {
                            return GetFilterValue(ref reader, in messageBody, JsonTokenType.EndObject);
                        }
                    }
                }
            }

            return string.Empty;
        }
        private static string GetFilterValue(ref Utf8JsonReader reader, in ReadOnlyMemory<byte> messageBody, JsonTokenType expectedToken)
        {
            int depth = reader.CurrentDepth;
            int start = Convert.ToInt32(reader.BytesConsumed) - 1;

            while (reader.Read())
            {
                if (reader.TokenType == expectedToken && reader.CurrentDepth == depth)
                {
                    int end = Convert.ToInt32(reader.BytesConsumed);
                    
                    int length = end - start;

                    return Encoding.UTF8.GetString(messageBody.Slice(start, length).Span);
                }
            }
            
            return string.Empty;
        }
    }
}