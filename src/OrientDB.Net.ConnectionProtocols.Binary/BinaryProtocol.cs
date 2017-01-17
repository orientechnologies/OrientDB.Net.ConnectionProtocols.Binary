using System.Collections.Generic;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using OrientDB.Net.ConnectionProtocols.Binary.Contracts;
using OrientDB.Net.ConnectionProtocols.Binary.Command;
using System;
using OrientDB.Net.Core.Abstractions;
using OrientDB.Net.Core.Models;

namespace OrientDB.Net.ConnectionProtocols.Binary
{
    public class BinaryProtocol : IOrientDBConnectionProtocol<byte[]>, IDisposable
    {
        private static IOrientDBConnection _connection;
        private readonly DatabaseConnectionOptions _options;

        public BinaryProtocol(string hostName, string userName, string password, string databaseName, DatabaseType type, int port = 2480) : this(new DatabaseConnectionOptions
        {
            Database = databaseName,
            Type = type,
            HostName = hostName,
            Password = password,
            Port = port,
            UserName = userName
        })
        { }

        public BinaryProtocol(DatabaseConnectionOptions options)
        {
            _options = options;
        }

        private static IOrientDBConnection GetConnection(DatabaseConnectionOptions options, IOrientDBRecordSerializer<byte[]> serializer)
        {
            if (_connection == null)
            {
                var conn = new OrientDBBinaryConnection(options, serializer);
                conn.Open();
                _connection = conn;
            }
            return _connection;
        }

        private static OrientDBBinaryConnection GetConnection()
        {
            if (_connection == null)
                throw new NullReferenceException($"{nameof(_connection)} is null.");
            return (OrientDBBinaryConnection)_connection;
        }

        public IOrientDBCommandResult ExecuteCommand(string sql, IOrientDBRecordSerializer<byte[]> serializer)
        {
            IOrientDBConnection connection = GetConnection(_options, serializer);
            _connection.CreateCommand().Execute(sql);
            return new OrientDBCommandResult();
        }

        public IEnumerable<TResultType> ExecuteQuery<TResultType>(string sql, IOrientDBRecordSerializer<byte[]> serializer) where TResultType : OrientDBEntity
        {
            IOrientDBConnection connection = GetConnection(_options, serializer);
            IEnumerable<TResultType> results = connection.CreateCommand().Execute<TResultType>(sql);

            return results;
        }

        public void Dispose()
        {
            GetConnection().Dispose();
        }
    }
}
