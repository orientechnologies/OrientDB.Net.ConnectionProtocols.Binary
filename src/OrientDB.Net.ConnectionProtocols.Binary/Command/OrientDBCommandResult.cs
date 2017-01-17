using OrientDB.Net.Core.Abstractions;

namespace OrientDB.Net.ConnectionProtocols.Binary.Command
{
    public class OrientDBCommandResult : IOrientDBCommandResult
    {
        public int RecordsAffected
        {
            get
            {
                return 0; // This needs to be updated. I don't think this is correct any longer.
            }
        }
    }
}
