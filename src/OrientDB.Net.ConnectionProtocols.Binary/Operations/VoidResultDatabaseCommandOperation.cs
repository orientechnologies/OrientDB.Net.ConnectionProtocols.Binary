using Operations;
using OrientDB.Net.ConnectionProtocols.Binary.Contracts;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using System.IO;
using OrientDB.Net.ConnectionProtocols.Binary.Command;
using System.Reflection;
using OrientDB.Net.Core.Abstractions;
using System;

namespace OrientDB.Net.ConnectionProtocols.Binary.Operations
{
    internal class VoidResultDatabaseCommandOperation : IOrientDBOperation<VoidResult>
    {
        private readonly string _fetchPlan;
        private ConnectionMetaData _connectionMetaData;
        private string _query;
        private ICommandPayloadConstructorFactory _payloadFactory;
        private IOrientDBRecordSerializer<byte[]> _serializer;

        public VoidResultDatabaseCommandOperation(ICommandPayloadConstructorFactory payloadFactory, ConnectionMetaData metaData, IOrientDBRecordSerializer<byte[]> serializer, string query, string fetchPlan = "*:0")
        {
            if (payloadFactory == null)
                throw new ArgumentNullException($"{nameof(payloadFactory)} cannot be null.");
            if (metaData == null)
                throw new ArgumentNullException($"{nameof(metaData)} cannot be null.");
            if (serializer == null)
                throw new ArgumentNullException($"{nameof(serializer)} cannot be null.");
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException($"{nameof(query)} cannot be zero length or null.");

            _payloadFactory = payloadFactory;
            _connectionMetaData = metaData;
            _serializer = serializer;
            _query = query;
            _fetchPlan = fetchPlan;
        }

        public Request CreateRequest(int sessionId, byte[] token)
        {
            return _payloadFactory.CreatePayload(_query, _fetchPlan, _connectionMetaData).CreatePayloadRequest(sessionId, token);
        }

        public VoidResult Execute(BinaryReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException($"{nameof(reader)} cannot be null.");

            while (!EndOfStream(reader))
                reader.ReadByte(); // Need to actually parse this data. I don't like just ditching it :/

            return new VoidResult();
        }

        protected bool EndOfStream(BinaryReader reader)
        {
            if (reader == null)
                throw new ArgumentNullException($"{nameof(reader)} cannot be null.");

            return !(bool)reader.BaseStream.GetType().GetProperty("DataAvailable").GetValue(reader.BaseStream);
        }
    }
}
