/*
   Copyright 2022 GitHub Inc.
   See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package logic

// EventsStreamer has been replaced by the transaction-aware streaming
// in gomysql_reader.go (StreamTransactions / handleTransactionEvent).
// The coordinator now handles event dispatching and parallel application.
