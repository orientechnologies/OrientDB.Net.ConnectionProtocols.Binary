using Operations;
using OrientDB.Net.ConnectionProtocols.Binary.Contracts;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using System.IO;
using OrientDB.Net.ConnectionProtocols.Binary.Command;
using System.Reflection;
using OrientDB.Net.Core.Abstractions;

namespace OrientDB.Net.ConnectionProtocols.Binary.Operations
{
    internal class VoidResultDatabaseCommandOperation : IOrientDBOperation<VoidResult>
    {
        private readonly string _fetchPlan;
        private ConnectionMetaData _connectionMetaData;
        private string _query;
        private ICommandPayloadConstructorFactory _payloadFactory;
        private IOrientDBRecordSerializer<byte[]> _serializer;

        public VoidResultDatabaseCommandOperation(ICommandPayloadConstructorFactory _payloadFactory, ConnectionMetaData connectionMetaData, IOrientDBRecordSerializer<byte[]> _serializer, string query, string fetchPlan = "*:0")
        {
            this._payloadFactory = _payloadFactory;
            this._connectionMetaData = connectionMetaData;
            this._serializer = _serializer;
            this._query = query;
            _fetchPlan = fetchPlan;
        }

        public Request CreateRequest(int sessionId, byte[] token)
        {
            return _payloadFactory.CreatePayload(_query, _fetchPlan, _connectionMetaData).CreatePayloadRequest(sessionId, token);
        }

        public VoidResult Execute(BinaryReader reader)
        {
            while (!EndOfStream(reader))
                reader.ReadByte();

            return new VoidResult();
        }

        protected bool EndOfStream(BinaryReader reader)
        {
            return !(bool)reader.BaseStream.GetType().GetProperty("DataAvailable").GetValue(reader.BaseStream);
        }
    }
}
