using OrientDB.Net.ConnectionProtocols.Binary.Contracts;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using System.IO;
using OrientDB.Net.Core.Abstractions;

namespace OrientDB.Net.ConnectionProtocols.Binary.Operations
{
    internal class DatabaseCreateOperation : IOrientDBOperation<OrientDBBinaryConnection>
    {
        private readonly string _databaseName;
        private readonly DatabaseType _databaseType;
        private readonly StorageType _storageType;
        private readonly ConnectionMetaData _metaData;
        private readonly IOrientDBRecordSerializer<byte[]> _serializer;
        private readonly ServerConnectionOptions _options;

        public DatabaseCreateOperation(string databaseName, DatabaseType databaseType, StorageType storageType, ConnectionMetaData metaData, ServerConnectionOptions options, IOrientDBRecordSerializer<byte[]> serializer)
        {
            _databaseName = databaseName;
            _databaseType = databaseType;
            _storageType = storageType;
            _metaData = metaData;
            _options = options;
            _serializer = serializer;
        }

        public Request CreateRequest(int sessionId, byte[] token)
        {
            Request request = new Request(OperationMode.Synchronous, sessionId);

            request.AddDataItem((byte)OperationType.DB_CREATE);
            request.AddDataItem(request.SessionId);

            // operation specific fields
            request.AddDataItem(_databaseName);
            request.AddDataItem(_databaseType.ToString().ToLower());
            request.AddDataItem(_storageType.ToString().ToLower());
            if (_metaData.ProtocolVersion >= 36) request.AddDataItem(-1); //Send null string for non-incrmental backup option

            return request;
        }

        OrientDBBinaryConnection IOrientDBOperation<OrientDBBinaryConnection>.Execute(BinaryReader reader)
        {
            return new OrientDBBinaryConnection(new DatabaseConnectionOptions()
            {
                Database = _databaseName,
                HostName = _options.HostName,
                Password = _options.Password,
                PoolSize = _options.PoolSize,
                Port = _options.Port,
                Type = _databaseType,
                UserName = _options.UserName
            }, _serializer);
        }
    }
}
