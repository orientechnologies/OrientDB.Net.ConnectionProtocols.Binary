using OrientDB.Net.ConnectionProtocols.Binary.Contracts;
using System;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using OrientDB.Net.Core.Models;

namespace OrientDB.Net.ConnectionProtocols.Binary.Operations
{
    internal enum TransactionRecordType
    {
        Create,
        Update,
        Delete
    }

    internal class DatabaseTransactionRequest : IOrientDBRequest
    {
        private readonly OrientDBEntity _entity;
        private readonly TransactionRecordType _recordType;

        public DatabaseTransactionRequest(TransactionRecordType recordType, OrientDBEntity entity)
        {
            _entity = entity;
            _recordType = recordType;
        }

        public ORID RecordORID
        {
            get { return _entity.ORID; }
            set { _entity.ORID = value; }
        }

        public TransactionRecordType RecordType
        {
            get { return _recordType; }
        }

        public Request CreateRequest(int sessionId, byte[] token)
        {
            throw new NotImplementedException();
        }
    }
}
