using OrientDB.Net.ConnectionProtocols.Binary.Core;
using System.IO;

namespace OrientDB.Net.ConnectionProtocols.Binary.Contracts
{
    internal interface IOrientDBOperation<T>
    {
        Request CreateRequest(int sessionId, byte[] token);
        T Execute(BinaryReader reader);
    }
}
