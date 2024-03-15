// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// mssql operators.
var mssqlOperators = map[string]string{
	"exact":       "= ?",
	"iexact":      "= UPPER(?)",
	"contains":    "LIKE ?",
	"icontains":   "LIKE UPPER(?)",
	"gt":          "> ?",
	"gte":         ">= ?",
	"lt":          "< ?",
	"lte":         "<= ?",
	"eq":          "= ?",
	"ne":          "!= ?",
	"startswith":  "LIKE ?",
	"endswith":    "LIKE ?",
	"istartswith": "LIKE UPPER(?)",
	"iendswith":   "LIKE UPPER(?)",
}

// mssql column field types.
var mssqlTypes = map[string]string{
	"auto":            "IDENTITY(1,1) NOT NULL PRIMARY KEY",
	"pk":              "NOT NULL PRIMARY KEY",
	"bool":            "bit",
	"string":          "varchar(%d)",
	"string-char":     "char(%d)",
	"string-text":     "text",
	"time.Time-date":  "date",
	"time.Time":       "datetime",
	"int8":            "tinyint",
	"int16":           "smallint",
	"int32":           "int",
	"int64":           "bigint",
	"uint8":           "tinyint",
	"uint16":          "smallint",
	"uint32":          "int",
	"uint64":          "bigint",
	"float64":         "float(53)",
	"float64-decimal": "number(%d, %d)",
}

var (
	sqlServerKeywords = `ADD,EXTERNAL,NATIONAL,SUBSTRING,ALL,FETCH,NCHAR,SUM,ALTER,FILE,NEXT,SYMMETRIC,AND,FILEGROUP,NOCHECK,THEN,ANY,FILESTREAM,NONCLUSTERED,TO,AS,FILLFACTOR,NOT,TOP,ASC,FOR,NULL,TRAN,AUTHORIZATION,FOREIGN,NULLIF,TRIGGER,BACKUP,FREETEXT,NUMERIC,TRUNCATE,BEGIN,FREETEXTTABLE,OF,TRY_CONVERT,BETWEEN,FROM,OFF,TSEQUAL,BROWSE,FUNCTION,ON,UNION,BULK,GOTO,OPEN,UNIQUE,BY,GRANT,OPENDATASOURCE,UNPIVOT,CASCADE,GROUP,OPENQUERY,UPDATE,CASE,HAVING,OPENROWSET,UPDATETEXT,CHECK,HOLDLOCK,OPENXML,USE,CHECKPOINT,IDENTITY,OPTION,USER,CLOSE,IF,OR,VALUES,CLUSTERED,IN,ORDER,VARYING,COALESCE,INDEX,OUTER,VIEW,COLLATE,INNER,OVER,WAITFOR,COLUMN,INSERT,PERCENT,WHEN,COMMIT,INTERSECT,PIVOT,WHERE,COMPUTE,INTO,PLAN,WHILE,CONSTRAINT,IS,PRECISION,WITH,CONTAINS,ISNULL,PRIMARY,WITHIN GROUP,CONTAINSTABLE,JOIN,PRINT,WRITETEXT,CONTINUE,KEY,PROC,CONVERT,KILL,PROCEDURE,CREATE,LEFT,PUBLIC,CROSS,LIKE,RAISERROR,CURRENT,LINENO,READ,CURRENT_DATE,LOAD,READTEXT,CURRENT_TIME,MERGE,RECONFIGURE,CURRENT_TIMESTAMP,MINUTE,REFERENCES,CURSOR,MONEY,REPLICATION,DATABASE,NATIONAL,RESTORE,TYPE,DESC,key_type,interval`
	SqlServerKeywords map[string]struct{}
)

func init() {
	SqlServerKeywords = make(map[string]struct{}, 0)
	keywordList := strings.Split(sqlServerKeywords, ",")
	for _, keyword := range keywordList {
		SqlServerKeywords[strings.ToLower(keyword)] = struct{}{}
	}
}
func IsSqlServerKeyword(key string) bool {
	_, exists := SqlServerKeywords[key]
	return exists
}

// mssql dbBaser.
type dbBaseMssql struct {
	dbBase
}

var _ dbBaser = new(dbBaseMssql)

// Get mssql operator.
func (d *dbBaseMssql) OperatorSQL(operator string) string {
	return mssqlOperators[operator]
}

// mssql support updating joined record.
func (d *dbBaseMssql) SupportUpdateJoin() bool {
	return true
}

// mssql quote is ".
func (d *dbBaseMssql) TableQuote() string {
	return ""
}

func (d *dbBaseMssql) ReplaceMarks(query *string) {
	ss := strings.Split(*query, "?")
	if len(ss) > 1 {
		*query = ""
		for i := 0; i < len(ss)-1; i++ {
			*query = *query + ss[i] + "$" + strconv.FormatInt(int64(i+1), 10)
		}

		*query = *query + ss[len(ss)-1]
	}
}

// show table sql for mssql.
func (d *dbBaseMssql) ShowTablesQuery() string {
	sql := `SELECT
	TABLE_NAME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_TYPE = 'BASE TABLE'
	AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')`
	return sql
}

// show table Columns sql for mssql.
func (d *dbBaseMssql) ShowColumnsQuery(table string) string {
	sql := `select
	COLUMN_NAME,
	DATA_TYPE,
	IS_NULLABLE
from
	INFORMATION_SCHEMA.COLUMNS
where
	TABLE_SCHEMA not in ('sys', 'INFORMATION_SCHEMA')
	and TABLE_NAME = '%s';`
	return fmt.Sprintf(sql, table)
}

// Get column types of mssql.
func (d *dbBaseMssql) DbTypes() map[string]string {
	return mssqlTypes
}

// check index exist in mssql.
func (d *dbBaseMssql) IndexExists(ctx context.Context, db dbQuerier, table string, name string) bool {
	countIndexSql := `SELECT
	COUNT(*) AS IndexCount
FROM
	sys.indexes i
INNER JOIN sys.objects o ON
	i.object_id = o.object_id
WHERE
	o.type = 'U'
	AND o.name = '%s'
	AND i.name = '%s'`
	query := fmt.Sprintf(countIndexSql, table, name)
	row := db.QueryRowContext(ctx, query)
	var cnt int
	row.Scan(&cnt)
	return cnt > 0
}

// GenerateSpecifyIndex return a specifying index clause
func (d *dbBaseMssql) GenerateSpecifyIndex(tableName string, useIndex int, indexes []string) string {
	DebugLog.Println("[WARN] Not support any specifying index action, so that action is ignored")
	return ``
}

// create new mssql dbBaser.
func newdbBaseMssql() dbBaser {
	b := new(dbBaseMssql)
	b.ins = b
	return b
}

func (d *dbBaseMssql) HasReturningID(mi *modelInfo, query *string) bool {
	return false
}
