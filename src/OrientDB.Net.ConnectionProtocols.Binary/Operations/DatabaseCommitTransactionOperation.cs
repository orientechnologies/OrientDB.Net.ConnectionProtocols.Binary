using OrientDB.Net.ConnectionProtocols.Binary.Contracts;
using OrientDB.Net.ConnectionProtocols.Binary.Operations.Results;
using System;
using OrientDB.Net.ConnectionProtocols.Binary.Core;
using System.IO;
using OrientDB.Net.ConnectionProtocols.Binary.Constants;
using System.Collections.Generic;

namespace OrientDB.Net.ConnectionProtocols.Binary.Operations
{
    internal class DatabaseCommitTransactionOperation : IOrientDBOperation<TransactionResult>
    {
        private readonly ConnectionMetaData _metaData;
        private readonly bool _useTransactionLog;
        private readonly IEnumerable<DatabaseTransactionRequest> _records;

        public DatabaseCommitTransactionOperation(IEnumerable<DatabaseTransactionRequest> records, ConnectionMetaData metaData, bool useTransactionLog)
        {
            _metaData = metaData;
            _useTransactionLog = useTransactionLog;
            _records = records;
        }

        public Request CreateRequest(int sessionId, byte[] token)
        {
            var request = new Request(OperationMode.Synchronous);

            request.AddDataItem((byte)OperationType.TX_COMMIT);
            request.AddDataItem(sessionId);

            if (DriverConstants.ProtocolVersion > 26 && _metaData.UseTokenBasedSession)
            {
                request.AddDataItem(token);
            }

            int transactionId = 1;
            request.AddDataItem(transactionId);
            request.AddDataItem((byte)(_useTransactionLog ? 1 : 0));

            foreach(var item in _records)
            {
                item.AddToRequest(request);
            }

            request.AddDataItem((byte)0);
            request.AddDataItem((int)0);

            return request;
        }

        private void AddToRequest(DatabaseTransactionRequest item)
        {
            throw new NotImplementedException();
        }

        public TransactionResult Execute(BinaryReader reader)
        {
            throw new NotImplementedException();
        }
    }
}
