using OrientDB.Net.ConnectionProtocols.Binary.Core;
using OrientDB.Net.Core.Abstractions;
using System;

namespace OrientDB.Net.ConnectionProtocols.Binary.Command
{
    internal class CommandPayloadConstructorFactory : ICommandPayloadConstructorFactory
    {
        private readonly IOrientDBLogger _logger;

        public CommandPayloadConstructorFactory(IOrientDBLogger logger)
        {
            _logger = logger ?? throw new ArgumentNullException($"{nameof(logger)} cannt be null.");
        }

        public ICommandPayload CreatePayload(string query, string fetchPlan, ConnectionMetaData metaData)
        {
            if (query.ToLower().StartsWith("select"))
                return new SelectCommandPayload(query, fetchPlan, metaData, _logger);
            if (query.ToLower().StartsWith("insert"))
                return new InsertCommandPayload(query, fetchPlan, metaData, _logger);
            if(query.ToLower().StartsWith("create")) // Maybe we really don't need a bunch of different types here.
                return new InsertCommandPayload(query, fetchPlan, metaData, _logger); // This works...

            return null;
        }
    }
}
