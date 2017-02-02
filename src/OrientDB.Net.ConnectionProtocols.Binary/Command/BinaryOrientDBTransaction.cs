using System;
using OrientDB.Net.Core.Abstractions;
using OrientDB.Net.Core.Models;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using System.Collections.Generic;
using OrientDB.Net.ConnectionProtocols.Binary.Operations;
using OrientDB.Net.Core.Exceptions;
using OrientDB.Net.ConnectionProtocols.Binary.Operations.Results;
using System.Linq;

namespace OrientDB.Net.ConnectionProtocols.Binary.Command
{
    public class BinaryOrientDBTransaction : IOrientDBTransaction
    {
        private readonly OrientDBBinaryConnectionStream _stream;
        private readonly Dictionary<ORID, DatabaseTransactionRequest> _records = new Dictionary<ORID, DatabaseTransactionRequest>();
        private readonly IOrientDBRecordSerializer<byte[]> _serializer;
        private readonly ConnectionMetaData _metaData;
        private readonly OrientDBBinaryConnectionStream _connectionStream;

        public BinaryOrientDBTransaction(OrientDBBinaryConnectionStream stream, IOrientDBRecordSerializer<byte[]> serializer, 
            ConnectionMetaData metaData, OrientDBBinaryConnectionStream connectionStream)
        {
            _stream = stream;
            _serializer = serializer;
            _metaData = metaData;
            _connectionStream = connectionStream;
        }

        public void AddEntity<T>(T entity) where T : OrientDBEntity
        {
            var record = new DatabaseTransactionRequest(TransactionRecordType.Create, entity, _serializer);
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
            TransactionResult tranResult = _connectionStream.Send(new DatabaseCommitTransactionOperation(_records.Values, _metaData, true));

            var survivingRecords = _records.Values.Where(r => r.RecordType != TransactionRecordType.Delete).ToList();

            foreach (var kvp in tranResult.CreatedRecordMapping)
            {
                var record = _records[kvp.Key];
                record.RecordORID = kvp.Value;
                _records.Add(record.RecordORID, record);                
            }

            var versions = tranResult.UpdatedRecordVersions;
            foreach (var kvp in versions)
            {
                var record = _records[kvp.Key];
                record.Version = kvp.Value;
            }

            Reset();
        }

        public void Reset()
        {
            _records.Clear();
        }
    }
}
