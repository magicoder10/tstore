package mutation

//
//import (
//	"encoding/json"
//	"fmt"
//	"io/ioutil"
//	"os"
//	"path/filepath"
//	"strconv"
//	"strings"
//
//	"tstore/storage"
//)
//
//var transactionLogFilePathFmt = filepath.Join(storage.DataDir, "databases", "%s", "transaction.log")
//
//var transactionsDirPathFmt = filepath.Join(storage.DataDir, "databases", "%s", "transactions")
//var transactionFilePathFmt = filepath.Join("%s", "%v.json")
//
//type TransactionStorage struct {
//	transactionLogFilePath string
//	transactionsDirPath    string
//}
//
//func (t TransactionStorage) ReadTransactions() ([]Transaction, error) {
//	fileNames, err := storage.ListFileNames(t.transactionsDirPath, ".json")
//	if err != nil {
//		return nil, err
//	}
//
//	var transactions []Transaction
//	for _, fileName := range fileNames {
//		idStr := fileName[0:strings.LastIndex(fileName, ".")]
//		id, err := strconv.ParseUint(idStr, 10, 64)
//		if err != nil {
//			continue
//		}
//
//		ts, err := t.ReadTransaction(id)
//		if err != nil {
//			continue
//		}
//
//		transactions = append(transactions, ts)
//	}
//
//	return transactions, nil
//}
//
//func (t TransactionStorage) ReadTransaction(transactionID uint64) (Transaction, error) {
//	buf, err := ioutil.ReadFile(fmt.Sprintf(transactionFilePathFmt, t.transactionsDirPath, transactionID))
//	if err != nil {
//		return Transaction{}, err
//	}
//
//	var trans Transaction
//	err = json.Unmarshal(buf, &trans)
//	return trans, err
//}
//
//func (t TransactionStorage) WriteTransaction(transaction Transaction) error {
//	buf, err := json.MarshalIndent(transaction, "", storage.JSONIndent)
//	if err != nil {
//		return err
//	}
//
//	return ioutil.WriteFile(fmt.Sprintf(transactionFilePathFmt, t.transactionsDirPath, transaction.ID), buf, storage.DefaultFileMode)
//}
//
//func (t TransactionStorage) WriteTransactionLog(logLine LogLine) error {
//	file, err := os.OpenFile(t.transactionLogFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, storage.DefaultFileMode)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//	_, err = file.WriteString(logLine.Line())
//	return err
//}
//
//func NewTransactionStorage(dbName string) (TransactionStorage, error) {
//	transactionsDirPath := fmt.Sprintf(transactionsDirPathFmt, dbName)
//	err := os.MkdirAll(transactionsDirPath, storage.DefaultFileMode)
//	if err != nil {
//		return TransactionStorage{}, err
//	}
//
//	return TransactionStorage{
//		transactionsDirPath:    transactionsDirPath,
//		transactionLogFilePath: fmt.Sprintf(transactionLogFilePathFmt, dbName),
//	}, nil
//}
//
//var idGenDirFmt = filepath.Join(storage.DataDir, "databases", "%s", "idGens")
//var idGenPathFmt = filepath.Join("%s", "%v.json")
//
//type IDGenStorage struct {
//	stateFilePath string
//}
//
//func (i IDGenStorage) ReadNextID() (uint64, error) {
//	buf, err := ioutil.ReadFile(i.stateFilePath)
//	if err != nil {
//		return 1, nil
//	}
//
//	savedNextID, err := strconv.ParseUint(string(buf), 10, 64)
//	if err != nil {
//		return 1, err
//	}
//
//	return savedNextID, nil
//}
//
//func (i IDGenStorage) WriteNextID(nextID uint64) error {
//	return ioutil.WriteFile(i.stateFilePath, ([]byte)(strconv.FormatUint(nextID, 10)), storage.DefaultFileMode)
//}
//
//func NewIDGenStorage(stateFilePath string) IDGenStorage {
//	return IDGenStorage{
//		stateFilePath: stateFilePath,
//	}
//}
