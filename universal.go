// 接口定义
package aliexhbase

import (
	"context"

	"github.com/Golang-Tools/aliexhbase/gen-go/hbase"
)

type UniversalClient interface {
	// Test for the existence of columns in the table, as specified in the TGet.
	//
	// @return true if the specified TGet matches one or more keys, false if not
	//
	// Parameters:
	//  - Table: the table to check on
	//  - Tget: the TGet to check for
	Exists(context.Context, []byte, *hbase.TGet) (bool, error)

	// Test for the existence of columns in the table, as specified by the TGets.
	//
	// This will return an array of booleans. Each value will be true if the related Get matches
	// one or more keys, false if not.
	//
	// Parameters:
	//  - Table: the table to check on
	//  - Tgets: a list of TGets to check for
	ExistsAll(context.Context, []byte, []*hbase.TGet) ([]bool, error)

	// Method for getting data from a row.
	//
	// If the row cannot be found an empty Result is returned.
	// This can be checked by the empty field of the TResult
	//
	// @return the result
	//
	// Parameters:
	//  - Table: the table to get from
	//  - Tget: the TGet to fetch
	Get(context.Context, []byte, *hbase.TGet) (*hbase.TResult_, error)
	// Method for getting multiple rows.
	//
	// If a row cannot be found there will be a null
	// value in the result list for that TGet at the
	// same position.
	//
	// So the Results are in the same order as the TGets.
	//
	// Parameters:
	//  - Table: the table to get from
	//  - Tgets: a list of TGets to fetch, the Result list
	// will have the Results at corresponding positions
	// or null if there was an error
	GetMultiple(context.Context, []byte, []*hbase.TGet) ([]*hbase.TResult_, error)

	// Commit a TPut to a table.
	//
	// Parameters:
	//  - Table: the table to put data in
	//  - Tput: the TPut to put
	Put(context.Context, []byte, *hbase.TPut) error

	// Atomically checks if a row/family/qualifier value matches the expected
	// value. If it does, it adds the TPut.
	//
	// @return true if the new put was executed, false otherwise
	//
	// Parameters:
	//  - Table: to check in and put to
	//  - Row: row to check
	//  - Family: column family to check
	//  - Qualifier: column qualifier to check
	//  - Value: the expected value, if not provided the
	// check is for the non-existence of the
	// column in question
	//  - Tput: the TPut to put if the check succeeds
	CheckAndPut(context.Context, []byte, []byte, []byte, []byte, []byte, *hbase.TPut) (bool, error)

	// Commit a List of Puts to the table.
	//
	// Parameters:
	//  - Table: the table to put data in
	//  - Tputs: a list of TPuts to commit
	PutMultiple(context.Context, []byte, []*hbase.TPut) error

	// Deletes as specified by the TDelete.
	//
	// Note: "delete" is a reserved keyword and cannot be used in Thrift
	// thus the inconsistent naming scheme from the other functions.
	//
	// Parameters:
	//  - Table: the table to delete from
	//  - Tdelete: the TDelete to delete
	DeleteSingle(context.Context, []byte, *hbase.TDelete) error

	// Bulk commit a List of TDeletes to the table.
	//
	// Throws a TIOError if any of the deletes fail.
	//
	// Always returns an empty list for backwards compatibility.
	//
	// Parameters:
	//  - Table: the table to delete from
	//  - Tdeletes: list of TDeletes to delete
	DeleteMultiple(context.Context, []byte, []*hbase.TDelete) ([]*hbase.TDelete, error)

	// Atomically checks if a row/family/qualifier value matches the expected
	// value. If it does, it adds the delete.
	//
	// @return true if the new delete was executed, false otherwise
	//
	// Parameters:
	//  - Table: to check in and delete from
	//  - Row: row to check
	//  - Family: column family to check
	//  - Qualifier: column qualifier to check
	//  - Value: the expected value, if not provided the
	// check is for the non-existence of the
	// column in question
	//  - Tdelete: the TDelete to execute if the check succeeds
	CheckAndDelete(context.Context, []byte, []byte, []byte, []byte, []byte, *hbase.TDelete) (bool, error)

	// Parameters:
	//  - Table: the table to increment the value on
	//  - Tincrement: the TIncrement to increment
	Increment(context.Context, []byte, *hbase.TIncrement) (*hbase.TResult_, error)

	// Parameters:
	//  - Table: the table to append the value on
	//  - Tappend: the TAppend to append
	Append(context.Context, []byte, *hbase.TAppend) (*hbase.TResult_, error)

	// Get a Scanner for the provided TScan object.
	//
	// @return Scanner Id to be used with other scanner procedures
	//
	// Parameters:
	//  - Table: the table to get the Scanner for
	//  - Tscan: the scan object to get a Scanner for
	OpenScanner(context.Context, []byte, *hbase.TScan) (int32, error)

	// Grabs multiple rows from a Scanner.
	//
	// @return Between zero and numRows TResults
	//
	// Parameters:
	//  - ScannerId: the Id of the Scanner to return rows from. This is an Id returned from the openScanner function.
	//  - NumRows: number of rows to return
	GetScannerRows(context.Context, int32, int32) ([]*hbase.TResult_, error)
	// Closes the scanner. Should be called to free server side resources timely.
	// Typically close once the scanner is not needed anymore, i.e. after looping
	// over it to get all the required rows.
	//
	// Parameters:
	//  - ScannerId: the Id of the Scanner to close *
	CloseScanner(context.Context, int32) error

	// mutateRow performs multiple mutations atomically on a single row.
	//
	// Parameters:
	//  - Table: table to apply the mutations
	//  - TrowMutations: mutations to apply
	MutateRow(context.Context, []byte, *hbase.TRowMutations) error

	// Get results for the provided TScan object.
	// This helper function opens a scanner, get the results and close the scanner.
	//
	// @return between zero and numRows TResults
	//
	// Parameters:
	//  - Table: the table to get the Scanner for
	//  - Tscan: the scan object to get a Scanner for
	//  - NumRows: number of rows to return

	GetScannerResults(context.Context, []byte, *hbase.TScan, int32) ([]*hbase.TResult_, error)

	// Given a table and a row get the location of the region that
	// would contain the given row key.
	//
	// reload = true means the cache will be cleared and the location
	// will be fetched from meta.
	//
	// Parameters:
	//  - Table
	//  - Row
	//  - Reload
	GetRegionLocation(context.Context, []byte, []byte, bool) (*hbase.THRegionLocation, error)

	// Get all of the region locations for a given table.
	//
	//
	// Parameters:
	//  - Table
	GetAllRegionLocations(context.Context, []byte) ([]*hbase.THRegionLocation, error)

	// Atomically checks if a row/family/qualifier value matches the expected
	// value. If it does, it mutates the row.
	//
	// @return true if the row was mutated, false otherwise
	//
	// Parameters:
	//  - Table: to check in and delete from
	//  - Row: row to check
	//  - Family: column family to check
	//  - Qualifier: column qualifier to check
	//  - CompareOp: comparison to make on the value
	//  - Value: the expected value to be compared against, if not provided the
	// check is for the non-existence of the column in question
	//  - RowMutations: row mutations to execute if the value matches
	CheckAndMutate(context.Context, []byte, []byte, []byte, []byte, hbase.TCompareOp, []byte, *hbase.TRowMutations) (bool, error)

	// Get a table descriptor.
	// @return the TableDescriptor of the giving tablename
	//
	//
	// Parameters:
	//  - Table: the tablename of the table to get tableDescriptor
	GetTableDescriptor(context.Context, *hbase.TTableName) (*hbase.TTableDescriptor, error)

	// Get table descriptors of tables.
	// @return the TableDescriptor of the giving tablename
	//
	//
	// Parameters:
	//  - Tables: the tablename list of the tables to get tableDescriptor
	GetTableDescriptors(context.Context, []*hbase.TTableName) ([]*hbase.TTableDescriptor, error)

	//
	// @return true if table exists already, false if not
	//
	//
	// Parameters:
	//  - TableName: the tablename of the tables to check
	TableExists(context.Context, *hbase.TTableName) (bool, error)

	// Get table descriptors of tables that match the given pattern
	// @return the tableDescriptors of the matching table
	//
	//
	// Parameters:
	//  - Regex: The regular expression to match against
	//  - IncludeSysTables: set to false if match only against userspace tables
	GetTableDescriptorsByPattern(context.Context, string, bool) ([]*hbase.TTableDescriptor, error)

	// Get table descriptors of tables in the given namespace
	// @return the tableDescriptors in the namespce
	//
	//
	// Parameters:
	//  - Name: The namesapce's name
	GetTableDescriptorsByNamespace(context.Context, string) ([]*hbase.TTableDescriptor, error)

	// Get table names of tables that match the given pattern
	// @return the table names of the matching table
	//
	//
	// Parameters:
	//  - Regex: The regular expression to match against
	//  - IncludeSysTables: set to false if match only against userspace tables
	GetTableNamesByPattern(context.Context, string, bool) ([]*hbase.TTableName, error)

	// Get table names of tables in the given namespace
	// @return the table names of the matching table
	//
	//
	// Parameters:
	//  - Name: The namesapce's name
	GetTableNamesByNamespace(context.Context, string) ([]*hbase.TTableName, error)
	// Creates a new table with an initial set of empty regions defined by the specified split keys.
	// The total number of regions created will be the number of split keys plus one. Synchronous
	// operation.
	//
	//
	// Parameters:
	//  - Desc: table descriptor for table
	//  - SplitKeys: rray of split keys for the initial regions of the table
	CreateTable(context.Context, *hbase.TTableDescriptor, [][]byte) error

	// Deletes a table. Synchronous operation.
	//
	//
	// Parameters:
	//  - TableName: the tablename to delete
	DeleteTable(context.Context, *hbase.TTableName) error

	// Truncate a table. Synchronous operation.
	//
	//
	// Parameters:
	//  - TableName: the tablename to truncate
	//  - PreserveSplits: whether to  preserve previous splits
	TruncateTable(context.Context, *hbase.TTableName, bool) error

	// Enalbe a table
	//
	//
	// Parameters:
	//  - TableName: the tablename to enable
	EnableTable(context.Context, *hbase.TTableName) error

	// Disable a table
	//
	//
	// Parameters:
	//  - TableName: the tablename to disable
	DisableTable(context.Context, *hbase.TTableName) error

	//
	// @return true if table is enabled, false if not
	//
	//
	// Parameters:
	//  - TableName: the tablename to check
	IsTableEnabled(context.Context, *hbase.TTableName) (bool, error)
	//
	// @return true if table is disabled, false if not
	//
	//
	// Parameters:
	//  - TableName: the tablename to check
	IsTableDisabled(context.Context, *hbase.TTableName) (bool, error)

	//
	// @return true if table is available, false if not
	//
	//
	// Parameters:
	//  - TableName: the tablename to check
	IsTableAvailable(context.Context, *hbase.TTableName) (bool, error)

	//  * Use this api to check if the table has been created with the specified number of splitkeys
	//  * which was used while creating the given table. Note : If this api is used after a table's
	//  * region gets splitted, the api may return false.
	//  *
	//  * @return true if table is available, false if not
	// *
	//
	// Parameters:
	//  - TableName: the tablename to check
	//  - SplitKeys: keys to check if the table has been created with all split keys
	IsTableAvailableWithSplit(context.Context, *hbase.TTableName, [][]byte) (bool, error)

	// Add a column family to an existing table. Synchronous operation.
	//
	//
	// Parameters:
	//  - TableName: the tablename to add column family to
	//  - Column: column family descriptor of column family to be added
	AddColumnFamily(context.Context, *hbase.TTableName, *hbase.TColumnFamilyDescriptor) error

	// Delete a column family from a table. Synchronous operation.
	//
	//
	// Parameters:
	//  - TableName: the tablename to delete column family from
	//  - Column: name of column family to be deleted
	DeleteColumnFamily(context.Context, *hbase.TTableName, []byte) error

	// Modify an existing column family on a table. Synchronous operation.
	//
	//
	// Parameters:
	//  - TableName: the tablename to modify column family
	//  - Column: column family descriptor of column family to be modified
	ModifyColumnFamily(context.Context, *hbase.TTableName, *hbase.TColumnFamilyDescriptor) error

	// Modify an existing table
	//
	//
	// Parameters:
	//  - Desc: the descriptor of the table to modify
	ModifyTable(context.Context, *hbase.TTableDescriptor) error

	// Create a new namespace. Blocks until namespace has been successfully created or an exception is
	// thrown
	//
	//
	// Parameters:
	//  - NamespaceDesc: descriptor which describes the new namespace
	CreateNamespace(context.Context, *hbase.TNamespaceDescriptor) error

	// Modify an existing namespace.  Blocks until namespace has been successfully modified or an
	// exception is thrown
	//
	//
	// Parameters:
	//  - NamespaceDesc: descriptor which describes the new namespace
	ModifyNamespace(context.Context, *hbase.TNamespaceDescriptor) error

	// Delete an existing namespace. Only empty namespaces (no tables) can be removed.
	// Blocks until namespace has been successfully deleted or an
	// exception is thrown.
	//
	//
	// Parameters:
	//  - Name: namespace name
	DeleteNamespace(context.Context, string) error

	// Get a namespace descriptor by name.
	// @retrun the descriptor
	//
	//
	// Parameters:
	//  - Name: name of namespace descriptor
	GetNamespaceDescriptor(context.Context, string) (*hbase.TNamespaceDescriptor, error)

	// @return all namespaces
	//
	ListNamespaceDescriptors(context.Context) ([]*hbase.TNamespaceDescriptor, error)

	//Close 关闭客户端,默认如果设置请求超时则等待一个请求超时的时间,否则等待1s
	Close() error
	//Open 开启客户端
	Open() error
	//IsOpen 判断客户端是否已经开启
	IsOpen() bool

	//NewCtx 用于构造请求用的上下文
	NewCtx() (context.Context, context.CancelFunc)
}
