using Operations;
using OrientDB.Net.ConnectionProtocols.Binary.Operations;
using System;
using OrientDB.Net.Core.Abstractions;

namespace OrientDB.Net.ConnectionProtocols.Binary.Core
{
    public class OrientDBBinaryServerConnection : IDisposable
    {
        private readonly ServerConnectionOptions _options;
        private readonly IOrientDBRecordSerializer<byte[]> _serializer;
        private OrientDBBinaryConnectionStream _connectionStream;

        public OrientDBBinaryServerConnection(ServerConnectionOptions options, IOrientDBRecordSerializer<byte[]> serializer)
        {
            if (options == null)
                throw new ArgumentNullException($"{nameof(options)} cannot be null.");
            if (serializer == null)
                throw new ArgumentNullException($"{nameof(serializer)} cannot be null.");

            _options = options;
            _serializer = serializer;
        }

        public void Open()
        {
            _connectionStream = new OrientDBBinaryConnectionStream(_options);
            foreach(var stream in _connectionStream.StreamPool)
            {
                var _openResult = _connectionStream.Send(new ServerOpenOperation(_options, _connectionStream.ConnectionMetaData));
                stream.SessionId = _openResult.SessionId;
                stream.Token = _openResult.Token;
            }
        }

        public OrientDBBinaryConnection CreateDatabase(string database, DatabaseType type, StorageType storageType)
        {
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException($"{nameof(database)} cannot be null or zero length.");

            return _connectionStream.Send(new DatabaseCreateOperation(database, type, storageType, _connectionStream.ConnectionMetaData, _options, _serializer));
        }

        public void DropDatabase(string database, StorageType storageType)
        {
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException($"{nameof(database)} cannot be null or zero length.");

            _connectionStream.Send(new DatabaseDropOperation(database, storageType, _connectionStream.ConnectionMetaData, _options));
        }

        public bool DatabaseExists(string database, StorageType storageType)
        {
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException($"{nameof(database)} cannot be null or zero length.");

            return _connectionStream.Send(new DatabaseExistsOperation(database, storageType, _connectionStream.ConnectionMetaData, _options)).Exists;
        }

        public void Dispose()
        {
            _connectionStream.Close();
        }
    }
}
