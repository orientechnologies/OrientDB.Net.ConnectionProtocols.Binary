using OrientDB.Net.ConnectionProtocols.Binary.Core;

namespace OrientDB.Net.ConnectionProtocols.Binary.Command
{
    internal class CommandPayloadConstructorFactory : ICommandPayloadConstructorFactory
    {
        public ICommandPayload CreatePayload(string query, string fetchPlan, ConnectionMetaData metaData)
        {
            if (query.ToLower().StartsWith("select"))
                return new SelectCommandPayload(query, fetchPlan, metaData);
            if (query.ToLower().StartsWith("insert"))
                return new InsertCommandPayload(query, fetchPlan, metaData);
            if(query.ToLower().StartsWith("create")) // Maybe we really don't need a bunch of different types here.
                return new InsertCommandPayload(query, fetchPlan, metaData); // This works...

            return null;
        }
    }
}
