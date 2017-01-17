using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OrientDB.Net.ConnectionProtocols.Binary.Operations
{
    public class CloseDatabaseResult
    {
        public bool IsSuccess { get; }

        public CloseDatabaseResult(bool isSuccess)
        {
            IsSuccess = isSuccess;
        }
    }
}
