using OrientDB.Net.Core.Abstractions;
using System;

namespace OrientDB.Net.ConnectionProtocols.Binary.Command
{
    public class BasicCommandResult : IOrientDBCommandResult
    {
        public int RecordsAffected => throw new NotImplementedException();
    }
}
