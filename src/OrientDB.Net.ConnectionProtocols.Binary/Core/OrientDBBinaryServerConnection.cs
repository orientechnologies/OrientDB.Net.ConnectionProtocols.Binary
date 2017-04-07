using Operations;
using OrientDB.Net.ConnectionProtocols.Binary.Operations;
using System;
using OrientDB.Net.Core.Abstractions;
using System.Collections.Generic;
using OrientDB.Net.Core.Models;

namespace OrientDB.Net.ConnectionProtocols.Binary.Core
{
    public class OrientDBBinaryServerConnection : IOrientServerConnection
    {
        private readonly ServerConnectionOptions _options;
        private readonly IOrientDBRecordSerializer<byte[]> _serializer;
        private OrientDBBinaryConnectionStream _connectionStream;
        private readonly IOrientDBLogger _logger;

        public OrientDBBinaryServerConnection(ServerConnectionOptions options, IOrientDBRecordSerializer<byte[]> serializer, IOrientDBLogger logger)
        {
            _logger = logger;

            _logger.Debug("OrientDBBinaryServerConnection.Ctor()");

            if (options == null)
                throw new ArgumentNullException($"{nameof(options)} cannot be null.");
            if (serializer == null)
                throw new ArgumentNullException($"{nameof(serializer)} cannot be null.");

            _options = options;
            _serializer = serializer;

            Open();
        }

        public void Open()
        {
            _connectionStream = new OrientDBBinaryConnectionStream(_options, _logger);
            foreach(var stream in _connectionStream.StreamPool)
            {
                var _openResult = _connectionStream.Send(new ServerOpenOperation(_options, _connectionStream.ConnectionMetaData));
                stream.SessionId = _openResult.SessionId;
                stream.Token = _openResult.Token;
            }
        }     

        public void Dispose()
        {
            _connectionStream.Close();
        }

        public IOrientDatabaseConnection CreateDatabase(string database, DatabaseType databaseType, StorageType storageType)
        {
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException($"{nameof(database)} cannot be null or zero length.");

            return _connectionStream.Send(new DatabaseCreateOperation(database, databaseType, storageType, _connectionStream.ConnectionMetaData, _options, _serializer, _logger));
        }

        public IOrientDatabaseConnection DatabaseConnect(string database, DatabaseType type, int poolSize = 10)
        {
            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException($"{nameof(database)} cannot be null or zero length.");

            return new OrientDBBinaryConnection(new DatabaseConnectionOptions
            {
                Database = database,
                HostName = _options.HostName,
                Password = _options.Password,
                PoolSize = poolSize,
                Port = _options.Port,
                Type = type,
                UserName = _options.UserName
            }, _serializer, _logger);
        }

        public void DeleteDatabase(string database, StorageType storageType)
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

        public void Shutdown(string username, string password)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> ListDatabases()
        {
            throw new NotImplementedException();
        }

        public string GetConfigValue(string name)
        {
            throw new NotImplementedException();
        }

        public void SetConfigValue(string name, string value)
        {
            throw new NotImplementedException();
        }
    }
}
