using OrientDB.Net.ConnectionProtocols.Binary.Command;
using OrientDB.Net.ConnectionProtocols.Binary.Contracts;
using OrientDB.Net.ConnectionProtocols.Binary.Operations;
using System;
using OrientDB.Net.Core.Abstractions;
using System.Linq;
using OrientDB.Net.ConnectionProtocols.Binary.Operations.Results;

namespace OrientDB.Net.ConnectionProtocols.Binary.Core
{
    public class OrientDBBinaryConnection : IOrientDBConnection, IDisposable
    {
        private readonly IOrientDBRecordSerializer<byte[]> _serialier;
        private readonly DatabaseConnectionOptions _connectionOptions;
        private OrientDBBinaryConnectionStream _connectionStream;
        private OpenDatabaseResult _openResult; // might not be how I model this here in the end.
        private ICommandPayloadConstructorFactory _payloadFactory;


        public OrientDBBinaryConnection(DatabaseConnectionOptions options, IOrientDBRecordSerializer<byte[]> serializer)
        {
            if (options == null)
                throw new ArgumentNullException($"{nameof(options)} cannot be null.");
            if (serializer == null)
                throw new ArgumentNullException($"{nameof(serializer)} cannot be null.");

            _connectionOptions = options;
            _serialier = serializer;
            _payloadFactory = new CommandPayloadConstructorFactory();
        }

        public OrientDBBinaryConnection(string hostname, string username, string password, IOrientDBRecordSerializer<byte[]> serializer, int port = 2424, int poolsize = 10)
        {
            if (string.IsNullOrWhiteSpace(hostname))
                throw new ArgumentException($"{nameof(hostname)} cannot be null or zero length.");
            if (string.IsNullOrWhiteSpace(username))
                throw new ArgumentException($"{nameof(username)} cannot be null or zero length.");
            if (string.IsNullOrWhiteSpace(password))
                throw new ArgumentException($"{nameof(password)} cannot be null or zero length.");
            if (serializer == null)
                throw new ArgumentNullException($"{nameof(serializer)} cannot be null.");

            _serialier = serializer;
            _connectionOptions = new DatabaseConnectionOptions
            {
                HostName = hostname,
                Password = password,
                PoolSize = poolsize,
                Port = port,
                UserName = username
            };
        }

        public void Open()
        {
            _connectionStream = new OrientDBBinaryConnectionStream(_connectionOptions);
            foreach(var stream in _connectionStream.StreamPool)
            {
                _openResult = _connectionStream.Send(new DatabaseOpenOperation(_connectionOptions, _connectionStream.ConnectionMetaData));
                stream.SessionId = _openResult.SessionId;
                stream.Token = _openResult.Token;
            }
        }

        public void Close()
        {
            _connectionStream.Send(new DatabaseCloseOperation(_openResult.Token, _connectionStream.ConnectionMetaData));
            _connectionStream.Close();
        }

        public IOrientDBCommand CreateCommand()
        {
            return new OrientDBCommand(_connectionStream, _serialier, _payloadFactory);
        }

        public void Dispose()
        {
            Close();
        }

        public IOrientDBTransaction CreateTransaction()
        {
            return new BinaryOrientDBTransaction(_connectionStream, _serialier, _connectionStream.ConnectionMetaData, (clusterName) =>
            {
                var schema = CreateCommand().Execute<ClassSchema>($"select expand(classes) from metadata:schema").First(n => n.Name == clusterName);
                return schema.DefaultClusterId;
            });
        }
    }
}
