using OrientDB.Net.Core.Models;
using System.Collections.Generic;

namespace OrientDB.Net.ConnectionProtocols.Binary.Contracts
{
    public interface IOrientDBCommand
    {
        IEnumerable<T> Execute<T>(string query) where T : OrientDBEntity;

        void Execute(string query);
    }
}