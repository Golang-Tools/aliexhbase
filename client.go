//客户端定义
package aliexhbase

import (
	"context"
	"net"
	"time"

	"github.com/Golang-Tools/aliexhbase/gen-go/hbase"
	"github.com/apache/thrift/lib/go/thrift"
)

type Client struct {
	pool *ThriftPool
	Opts Options
}

func New(opts ...Option) (*Client, error) {
	c := new(Client)
	c.Opts = DefaultOptions
	for _, opt := range opts {
		opt.Apply(&c.Opts)
	}
	if c.Opts.Poolconfig.Addr == "" || c.Opts.Poolconfig.User == "" || c.Opts.Poolconfig.Passwd == "" {
		return nil, ErrClientCreateParamsNotEnough
	}
	c.pool = NewThriftPool(c.Opts.Poolconfig)
	return c, nil
}

// NewCtx 根据注册的超时时间构造一个上下文
func (c *Client) NewCtx() (ctx context.Context, cancel context.CancelFunc) {
	if c.Opts.QueryTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), c.Opts.QueryTimeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	return
}

//Close 关闭客户端,默认如果设置请求超时则等待一个请求超时的时间,否则等待1s
func (c *Client) Close() error {
	err := c.HardClose()
	if err != nil {
		return err
	}
	if c.Opts.QueryTimeout > 0 {
		time.Sleep(c.Opts.QueryTimeout)
	} else {
		time.Sleep(DEFAULT_POOL_CLOSE_TIMEOUT)
	}
	return nil
}

//HardClose 强制关闭客户端
func (c *Client) HardClose() error {
	if c.pool == nil {
		return ErrClientPoolNotSet
	}
	if !c.pool.IsOpen() {
		return ErrPoolClosed
	}
	c.pool.Release()
	return nil
}

//SoftClose 设置等待时长以软关闭客户端
func (c *Client) SoftClose(timeout time.Duration) error {
	err := c.HardClose()
	if err != nil {
		return err
	}
	time.Sleep(timeout)
	return nil
}

//Open 开启客户端
func (c *Client) Open() error {
	if c.pool == nil {
		return ErrClientPoolNotSet
	}
	if c.pool.IsOpen() {
		return ErrPoolAlreadyOpened
	}
	c.pool.Recover()
	return nil
}

//IsOpen 判断客户端是否已经开启
func (c *Client) IsOpen() bool {
	if c.pool == nil {
		return false
	}
	return c.pool.IsOpen()
}

// do 通过闭包中调用来处理连接池中的连接对象的上下文
func (p *Client) do(fn func(conn *Conn) error) error {
	var (
		client *Conn
		err    error
	)
	defer func() {
		if client != nil {
			if err == nil {
				rErr := p.pool.Put(client)
				if rErr != nil {
					p.Opts.Logger.WithError(rErr).Error("Release Client error")
				}
			} else if _, ok := err.(net.Error); ok {
				p.pool.CloseConn(client)
			} else if _, ok = err.(thrift.TTransportException); ok {
				p.pool.CloseConn(client)
			} else {
				if rErr := p.pool.Put(client); rErr != nil {
					p.Opts.Logger.WithError(rErr).Error("Release Client error")
				}
			}
		}
	}()
	// 从连接池里获取链接
	client, err = p.pool.Get()
	if err != nil {
		return err
	}
	err = fn(client)
	if err != nil {
		_, ok := err.(net.Error)
		if ok {
			p.Opts.Logger.WithError(err).Error("Retry TCP error")
			// 网络错误，重建连接
			client, err = p.pool.Reconnect(client)
			if err != nil {
				return err
			}
			return fn(client)
		}

		_, ok = err.(thrift.TTransportException)
		if ok {
			p.Opts.Logger.WithError(err).Error("Retry TCP error")
			// thrift传输层错误，也重建连接
			client, err = p.pool.Reconnect(client)
			if err != nil {
				return err
			}
			return fn(client)
		}
		return err
	}
	return nil
}

// Test for the existence of columns in the table, as specified in the TGet.
//
// @return true if the specified TGet matches one or more keys, false if not
//
// Parameters:
//  - Table: the table to check on
//  - Tget: the TGet to check for
func (p *Client) Exists(ctx context.Context, table []byte, tget *hbase.TGet) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.Exists(ctx, table, tget)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

// Test for the existence of columns in the table, as specified by the TGets.
//
// This will return an array of booleans. Each value will be true if the related Get matches
// one or more keys, false if not.
//
// Parameters:
//  - Table: the table to check on
//  - Tgets: a list of TGets to check for
func (p *Client) ExistsAll(ctx context.Context, table []byte, tgets []*hbase.TGet) ([]bool, error) {
	var result []bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.ExistsAll(ctx, table, tgets)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

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
func (p *Client) Get(ctx context.Context, table []byte, tget *hbase.TGet) (*hbase.TResult_, error) {
	var tResult_ *hbase.TResult_
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.Get(ctx, table, tget)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

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
func (p *Client) GetMultiple(ctx context.Context, table []byte, tgets []*hbase.TGet) ([]*hbase.TResult_, error) {
	var tResult_ []*hbase.TResult_
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetMultiple(ctx, table, tgets)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Commit a TPut to a table.
//
// Parameters:
//  - Table: the table to put data in
//  - Tput: the TPut to put
func (p *Client) Put(ctx context.Context, table []byte, tput *hbase.TPut) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.Put(ctx, table, tput)
		return err2
	})
	if err != nil {
		return err
	}
	return nil
}

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
func (p *Client) CheckAndPut(ctx context.Context, table []byte, row []byte, family []byte, qualifier []byte, value []byte, tput *hbase.TPut) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.CheckAndPut(ctx, table, row, family, qualifier, value, tput)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

// Commit a List of Puts to the table.
//
// Parameters:
//  - Table: the table to put data in
//  - Tputs: a list of TPuts to commit
func (p *Client) PutMultiple(ctx context.Context, table []byte, tputs []*hbase.TPut) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.PutMultiple(ctx, table, tputs)
		return err2
	})
	return err
}

// Deletes as specified by the TDelete.
//
// Note: "delete" is a reserved keyword and cannot be used in Thrift
// thus the inconsistent naming scheme from the other functions.
//
// Parameters:
//  - Table: the table to delete from
//  - Tdelete: the TDelete to delete
func (p *Client) DeleteSingle(ctx context.Context, table []byte, tdelete *hbase.TDelete) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.DeleteSingle(ctx, table, tdelete)
		return err2
	})
	return err
}

// Bulk commit a List of TDeletes to the table.
//
// Throws a TIOError if any of the deletes fail.
//
// Always returns an empty list for backwards compatibility.
//
// Parameters:
//  - Table: the table to delete from
//  - Tdeletes: list of TDeletes to delete
func (p *Client) DeleteMultiple(ctx context.Context, table []byte, tdeletes []*hbase.TDelete) ([]*hbase.TDelete, error) {
	var tResult_ []*hbase.TDelete
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.DeleteMultiple(ctx, table, tdeletes)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

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
func (p *Client) CheckAndDelete(ctx context.Context, table []byte, row []byte, family []byte, qualifier []byte, value []byte, tdelete *hbase.TDelete) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.CheckAndDelete(ctx, table, row, family, qualifier, value, tdelete)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

// Parameters:
//  - Table: the table to increment the value on
//  - Tincrement: the TIncrement to increment
func (p *Client) Increment(ctx context.Context, table []byte, tincrement *hbase.TIncrement) (*hbase.TResult_, error) {
	var tResult_ *hbase.TResult_
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.Increment(ctx, table, tincrement)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Parameters:
//  - Table: the table to append the value on
//  - Tappend: the TAppend to append
func (p *Client) Append(ctx context.Context, table []byte, tappend *hbase.TAppend) (*hbase.TResult_, error) {
	var tResult_ *hbase.TResult_
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.Append(ctx, table, tappend)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Get a Scanner for the provided TScan object.
//
// @return Scanner Id to be used with other scanner procedures
//
// Parameters:
//  - Table: the table to get the Scanner for
//  - Tscan: the scan object to get a Scanner for
func (p *Client) OpenScanner(ctx context.Context, table []byte, tscan *hbase.TScan) (int32, error) {
	var tResult_ int32
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.OpenScanner(ctx, table, tscan)
		return err2
	})
	if err != nil {
		return 0, err
	}
	return tResult_, nil
}

// Grabs multiple rows from a Scanner.
//
// @return Between zero and numRows TResults
//
// Parameters:
//  - ScannerId: the Id of the Scanner to return rows from. This is an Id returned from the openScanner function.
//  - NumRows: number of rows to return
func (p *Client) GetScannerRows(ctx context.Context, scannerId int32, numRows int32) ([]*hbase.TResult_, error) {
	var tResult_ []*hbase.TResult_
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetScannerRows(ctx, scannerId, numRows)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Closes the scanner. Should be called to free server side resources timely.
// Typically close once the scanner is not needed anymore, i.e. after looping
// over it to get all the required rows.
//
// Parameters:
//  - ScannerId: the Id of the Scanner to close *
func (p *Client) CloseScanner(ctx context.Context, scannerId int32) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.CloseScanner(ctx, scannerId)
		return err2
	})
	return err
}

// mutateRow performs multiple mutations atomically on a single row.
//
// Parameters:
//  - Table: table to apply the mutations
//  - TrowMutations: mutations to apply
func (p *Client) MutateRow(ctx context.Context, table []byte, trowMutations *hbase.TRowMutations) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.MutateRow(ctx, table, trowMutations)
		return err2
	})
	return err
}

// Get results for the provided TScan object.
// This helper function opens a scanner, get the results and close the scanner.
//
// @return between zero and numRows TResults
//
// Parameters:
//  - Table: the table to get the Scanner for
//  - Tscan: the scan object to get a Scanner for
//  - NumRows: number of rows to return
func (p *Client) GetScannerResults(ctx context.Context, table []byte, tscan *hbase.TScan, numRows int32) ([]*hbase.TResult_, error) {
	var tResult_ []*hbase.TResult_
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetScannerResults(ctx, table, tscan, numRows)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

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
func (p *Client) GetRegionLocation(ctx context.Context, table []byte, row []byte, reload bool) (*hbase.THRegionLocation, error) {
	var tResult_ *hbase.THRegionLocation
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetRegionLocation(ctx, table, row, reload)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Get all of the region locations for a given table.
//
//
// Parameters:
//  - Table
func (p *Client) GetAllRegionLocations(ctx context.Context, table []byte) ([]*hbase.THRegionLocation, error) {
	var tResult_ []*hbase.THRegionLocation
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetAllRegionLocations(ctx, table)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

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
func (p *Client) CheckAndMutate(ctx context.Context, table []byte, row []byte, family []byte, qualifier []byte, compareOp hbase.TCompareOp, value []byte, rowMutations *hbase.TRowMutations) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.CheckAndMutate(ctx, table, row, family, qualifier, compareOp, value, rowMutations)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

// Get a table descriptor.
// @return the TableDescriptor of the giving tablename
//
//
// Parameters:
//  - Table: the tablename of the table to get tableDescriptor
func (p *Client) GetTableDescriptor(ctx context.Context, table *hbase.TTableName) (*hbase.TTableDescriptor, error) {
	var tResult_ *hbase.TTableDescriptor
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetTableDescriptor(ctx, table)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Get table descriptors of tables.
// @return the TableDescriptor of the giving tablename
//
//
// Parameters:
//  - Tables: the tablename list of the tables to get tableDescriptor
func (p *Client) GetTableDescriptors(ctx context.Context, tables []*hbase.TTableName) ([]*hbase.TTableDescriptor, error) {
	var tResult_ []*hbase.TTableDescriptor
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetTableDescriptors(ctx, tables)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

//
// @return true if table exists already, false if not
//
//
// Parameters:
//  - TableName: the tablename of the tables to check
func (p *Client) TableExists(ctx context.Context, tableName *hbase.TTableName) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.TableExists(ctx, tableName)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

// Get table descriptors of tables that match the given pattern
// @return the tableDescriptors of the matching table
//
//
// Parameters:
//  - Regex: The regular expression to match against
//  - IncludeSysTables: set to false if match only against userspace tables
func (p *Client) GetTableDescriptorsByPattern(ctx context.Context, regex string, includeSysTables bool) ([]*hbase.TTableDescriptor, error) {
	var tResult_ []*hbase.TTableDescriptor
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetTableDescriptorsByPattern(ctx, regex, includeSysTables)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Get table descriptors of tables in the given namespace
// @return the tableDescriptors in the namespce
//
//
// Parameters:
//  - Name: The namesapce's name
func (p *Client) GetTableDescriptorsByNamespace(ctx context.Context, name string) ([]*hbase.TTableDescriptor, error) {
	var tResult_ []*hbase.TTableDescriptor
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetTableDescriptorsByNamespace(ctx, name)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Get table names of tables that match the given pattern
// @return the table names of the matching table
//
//
// Parameters:
//  - Regex: The regular expression to match against
//  - IncludeSysTables: set to false if match only against userspace tables
func (p *Client) GetTableNamesByPattern(ctx context.Context, regex string, includeSysTables bool) ([]*hbase.TTableName, error) {
	var tResult_ []*hbase.TTableName
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetTableNamesByPattern(ctx, regex, includeSysTables)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Get table names of tables in the given namespace
// @return the table names of the matching table
//
//
// Parameters:
//  - Name: The namesapce's name
func (p *Client) GetTableNamesByNamespace(ctx context.Context, name string) ([]*hbase.TTableName, error) {
	var tResult_ []*hbase.TTableName
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetTableNamesByNamespace(ctx, name)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// Creates a new table with an initial set of empty regions defined by the specified split keys.
// The total number of regions created will be the number of split keys plus one. Synchronous
// operation.
//
//
// Parameters:
//  - Desc: table descriptor for table
//  - SplitKeys: rray of split keys for the initial regions of the table
func (p *Client) CreateTable(ctx context.Context, desc *hbase.TTableDescriptor, splitKeys [][]byte) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.CreateTable(ctx, desc, splitKeys)
		return err2
	})
	return err
}

// Deletes a table. Synchronous operation.
//
//
// Parameters:
//  - TableName: the tablename to delete
func (p *Client) DeleteTable(ctx context.Context, tableName *hbase.TTableName) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.DeleteTable(ctx, tableName)
		return err2
	})
	return err
}

// Truncate a table. Synchronous operation.
//
//
// Parameters:
//  - TableName: the tablename to truncate
//  - PreserveSplits: whether to  preserve previous splits
func (p *Client) TruncateTable(ctx context.Context, tableName *hbase.TTableName, preserveSplits bool) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.TruncateTable(ctx, tableName, preserveSplits)
		return err2
	})
	return err
}

// Enalbe a table
//
//
// Parameters:
//  - TableName: the tablename to enable
func (p *Client) EnableTable(ctx context.Context, tableName *hbase.TTableName) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.EnableTable(ctx, tableName)
		return err2
	})
	return err
}

// Disable a table
//
//
// Parameters:
//  - TableName: the tablename to disable
func (p *Client) DisableTable(ctx context.Context, tableName *hbase.TTableName) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.DisableTable(ctx, tableName)
		return err2
	})
	return err
}

//
// @return true if table is enabled, false if not
//
//
// Parameters:
//  - TableName: the tablename to check
func (p *Client) IsTableEnabled(ctx context.Context, tableName *hbase.TTableName) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.IsTableEnabled(ctx, tableName)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

//
// @return true if table is disabled, false if not
//
//
// Parameters:
//  - TableName: the tablename to check
func (p *Client) IsTableDisabled(ctx context.Context, tableName *hbase.TTableName) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.IsTableDisabled(ctx, tableName)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

//
// @return true if table is available, false if not
//
//
// Parameters:
//  - TableName: the tablename to check
func (p *Client) IsTableAvailable(ctx context.Context, tableName *hbase.TTableName) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.IsTableAvailable(ctx, tableName)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

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
func (p *Client) IsTableAvailableWithSplit(ctx context.Context, tableName *hbase.TTableName, splitKeys [][]byte) (bool, error) {
	var result bool
	err := p.do(func(conn *Conn) error {
		var err2 error
		result, err2 = conn.IsTableAvailableWithSplit(ctx, tableName, splitKeys)
		return err2
	})
	if err != nil {
		return false, err
	}
	return result, nil
}

// Add a column family to an existing table. Synchronous operation.
//
//
// Parameters:
//  - TableName: the tablename to add column family to
//  - Column: column family descriptor of column family to be added
func (p *Client) AddColumnFamily(ctx context.Context, tableName *hbase.TTableName, column *hbase.TColumnFamilyDescriptor) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.AddColumnFamily(ctx, tableName, column)
		return err2
	})
	return err
}

// Delete a column family from a table. Synchronous operation.
//
//
// Parameters:
//  - TableName: the tablename to delete column family from
//  - Column: name of column family to be deleted
func (p *Client) DeleteColumnFamily(ctx context.Context, tableName *hbase.TTableName, column []byte) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.DeleteColumnFamily(ctx, tableName, column)
		return err2
	})
	return err
}

// Modify an existing column family on a table. Synchronous operation.
//
//
// Parameters:
//  - TableName: the tablename to modify column family
//  - Column: column family descriptor of column family to be modified
func (p *Client) ModifyColumnFamily(ctx context.Context, tableName *hbase.TTableName, column *hbase.TColumnFamilyDescriptor) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.ModifyColumnFamily(ctx, tableName, column)
		return err2
	})
	return err
}

// Modify an existing table
//
//
// Parameters:
//  - Desc: the descriptor of the table to modify
func (p *Client) ModifyTable(ctx context.Context, desc *hbase.TTableDescriptor) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.ModifyTable(ctx, desc)
		return err2
	})
	return err
}

// Create a new namespace. Blocks until namespace has been successfully created or an exception is
// thrown
//
//
// Parameters:
//  - NamespaceDesc: descriptor which describes the new namespace
func (p *Client) CreateNamespace(ctx context.Context, namespaceDesc *hbase.TNamespaceDescriptor) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.CreateNamespace(ctx, namespaceDesc)
		return err2
	})
	return err
}

// Modify an existing namespace.  Blocks until namespace has been successfully modified or an
// exception is thrown
//
//
// Parameters:
//  - NamespaceDesc: descriptor which describes the new namespace
func (p *Client) ModifyNamespace(ctx context.Context, namespaceDesc *hbase.TNamespaceDescriptor) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.ModifyNamespace(ctx, namespaceDesc)
		return err2
	})
	return err
}

// Delete an existing namespace. Only empty namespaces (no tables) can be removed.
// Blocks until namespace has been successfully deleted or an
// exception is thrown.
//
//
// Parameters:
//  - Name: namespace name
func (p *Client) DeleteNamespace(ctx context.Context, name string) error {
	err := p.do(func(conn *Conn) error {
		err2 := conn.DeleteNamespace(ctx, name)
		return err2
	})
	return err
}

// Get a namespace descriptor by name.
// @retrun the descriptor
//
//
// Parameters:
//  - Name: name of namespace descriptor
func (p *Client) GetNamespaceDescriptor(ctx context.Context, name string) (*hbase.TNamespaceDescriptor, error) {
	var tResult_ *hbase.TNamespaceDescriptor
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.GetNamespaceDescriptor(ctx, name)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}

// @return all namespaces
//
func (p *Client) ListNamespaceDescriptors(ctx context.Context) ([]*hbase.TNamespaceDescriptor, error) {
	var tResult_ []*hbase.TNamespaceDescriptor
	err := p.do(func(conn *Conn) error {
		var err2 error
		tResult_, err2 = conn.ListNamespaceDescriptors(ctx)
		return err2
	})
	if err != nil {
		return nil, err
	}
	return tResult_, nil
}
