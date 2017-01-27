using System;
using OrientDB.Net.Core.Abstractions;
using OrientDB.Net.Core.Models;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using System.Collections.Generic;
using OrientDB.Net.ConnectionProtocols.Binary.Operations;
using OrientDB.Net.Core.Exceptions;

namespace OrientDB.Net.ConnectionProtocols.Binary.Command
{
    public class BinaryOrientDBTransaction : IOrientDBTransaction
    {
        private readonly OrientDBBinaryConnectionStream _stream;
        private readonly Dictionary<ORID, DatabaseTransactionRequest> _records = new Dictionary<ORID, DatabaseTransactionRequest>();

        public BinaryOrientDBTransaction(OrientDBBinaryConnectionStream stream)
        {
            _stream = stream;
        }

        public void AddEntity<T>(T entity) where T : OrientDBEntity
        {
            var record = new DatabaseTransactionRequest(TransactionRecordType.Create, entity);
            AddToRecords(record);   
        }

        private void AddToRecords(DatabaseTransactionRequest record)
        {
            bool hasOrid = record.RecordORID != null;
            bool needsOrid = record.RecordType != TransactionRecordType.Create;

            if(!hasOrid)
            {
                record.RecordORID = ORID.NewORID();
                record.RecordORID.ClusterId = 1; // Need to create logic to retrieve ClusterId for the record's class.
            }

            if (_records.ContainsKey(record.RecordORID))
            {
                if (record.RecordType != _records[record.RecordORID].RecordType)
                    throw new OrientDBException(OrientDBExceptionType.Query, "Record has already been added as part of another operation within this transaction."); // Fix the Exception Type.
                _records[record.RecordORID] = record;
            }
            else
                _records.Add(record.RecordORID, record);
        }

        public void Commit()
        {
            // Need to hash out the object interactions here.

            throw new NotImplementedException();
        }
    }
}
