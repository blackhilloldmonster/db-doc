package database

import (
	"database/sql"
	"db-doc/doc"
	"db-doc/model"
	"fmt"
	"os"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var dbConfig model.DbConfig

// Generate generate doc
func Generate(config *model.DbConfig) {
	dbConfig = *config
	db := initDB()
	if db == nil {
		fmt.Println("init database err")
		os.Exit(1)
	}
	defer db.Close()

	var dbInfo model.DbInfo

	if config.DbType == 1 {
		dbInfo = getDbInfo(db)
	}
	if config.DbType == 2 {
		dbInfo = getDbInfo(db)
	}
	if config.DbType == 3 {
		dbInfo = getPgDbInfo(db)
	}

	dbInfo.DbName = config.Database
	tables := getTableInfo(db)
	// create
	doc.CreateDoc(dbInfo, config.DocType, tables)
}

// InitDB 初始化数据库
func initDB() *sql.DB {
	var (
		dbURL  string
		dbType string
	)
	if dbConfig.DbType == 1 {
		// https://github.com/go-sql-driver/mysql/
		dbType = "mysql"
		// <username>:<password>@<host>:<port>/<database>
		dbURL = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
			dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)
	}
	if dbConfig.DbType == 2 {
		// https://github.com/denisenkom/go-mssqldb
		dbType = "mssql"
		// server=%s;database=%s;user id=%s;password=%s;port=%d;encrypt=disable
		dbURL = fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s;port=%d;encrypt=disable",
			dbConfig.Host, dbConfig.Database, dbConfig.User, dbConfig.Password, dbConfig.Port)
	}
	if dbConfig.DbType == 3 {
		// https://github.com/lib/pq
		dbType = "postgres"
		// postgres://pqgotest:password@localhost:5432/pqgotest?sslmode=verify-full
		dbURL = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", dbConfig.User, dbConfig.Password,
			dbConfig.Host, dbConfig.Port, dbConfig.Database)
	}
	db, err := sql.Open(dbType, dbURL)
	if err != nil {
		fmt.Println(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	return db
}

// getDbInfo 获取数据库的基本信息
func getDbInfo(db *sql.DB) model.DbInfo {
	var (
		info       model.DbInfo
		rows       *sql.Rows
		err        error
		key, value string
	)
	// 数据库版本
	rows, err = db.Query("select @@version;")
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&value)
	}
	info.Version = value
	// 字符集
	rows, err = db.Query("show variables like '%character_set_server%';")
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&key, &value)
	}
	info.Charset = value
	// 排序规则
	rows, err = db.Query("show variables like 'collation_server%';")
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&key, &value)
	}
	info.Collation = value
	return info
}

func getPgDbInfo(db *sql.DB) model.DbInfo {
	var (
		info  model.DbInfo
		rows  *sql.Rows
		err   error
		value string
	)
	// PostgreSQL中没有@@version这样的系统变量，我们使用替代的查询
	rows, err = db.Query("SHOW server_version;")
	if err != nil {
		fmt.Println(err)
		return info // 不要忘记返回空的info
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&value)
	}
	info.Version = value
	// 字符集，PostgreSQL中查询字符集的SQL语句不同
	rows, err = db.Query("SHOW server_encoding;")
	if err != nil {
		fmt.Println(err)
		return info // 不要忘记返回空的info
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&value)
	}
	info.Charset = value
	// // 排序规则，PostgreSQL中查询排序规则的SQL语句不同
	// err = db.QueryRow("SHOW lc_collate;").Scan(&value)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return info // 如果查询失败，返回空的info
	// }
	// info.Collation = value

	// 排序规则，PostgreSQL中查询排序规则的SQL语句不同
	rows, err = db.Query("SHOW lc_collate;")
	if err != nil {
		fmt.Println(err)
		return info // 不要忘记返回空的info
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&value)
	}
	info.Collation = value
	return info
}

// getTableInfo 获取表信息
func getTableInfo(db *sql.DB) []model.Table {
	// find all tables
	tables := make([]model.Table, 0)
	rows, err := db.Query(getTableSQL())
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	var table model.Table
	for rows.Next() {
		table.TableComment = ""
		rows.Scan(&table.TableName, &table.TableComment)
		if len(table.TableComment) == 0 {
			table.TableComment = table.TableName
		}
		tables = append(tables, table)
	}
	for i := range tables {
		columns := getColumnInfo(db, tables[i].TableName)
		tables[i].ColList = columns
	}
	return tables
}

// getColumnInfo 获取列信息
func getColumnInfo(db *sql.DB, tableName string) []model.Column {
	columns := make([]model.Column, 0)
	rows, err := db.Query(getColumnSQL(tableName))
	if err != nil {
		fmt.Println(err)
	}
	var column model.Column
	for rows.Next() {
		rows.Scan(&column.ColName, &column.ColType, &column.ColKey, &column.IsNullable, &column.ColComment, &column.ColDefault)
		columns = append(columns, column)
		column.ColDefault = ""
	}
	return columns
}

// getTableSQL
func getTableSQL() string {
	var sql string
	if dbConfig.DbType == 1 {
		sql = fmt.Sprintf(`
			select table_name    as TableName, 
			       table_comment as TableComment
			from information_schema.tables 
			where table_schema = '%s'
		`, dbConfig.Database)
	}
	if dbConfig.DbType == 2 {
		sql = fmt.Sprintf(`
		select * from (
			select cast(so.name as varchar(500)) as TableName, 
			cast(sep.value as varchar(500))      as TableComment
			from sysobjects so
			left JOIN sys.extended_properties sep on sep.major_id=so.id and sep.minor_id=0
			where (xtype='U' or xtype='v')
		) t 
		`)
	}
	if dbConfig.DbType == 3 {
		sql = fmt.Sprintf(`
			SELECT a.relname     as TableName, 
				   b.description as TableComment
			FROM pg_class a
			LEFT OUTER JOIN pg_description b ON b.objsubid = 0 AND a.oid = b.objoid
			WHERE a.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
			AND a.relkind = 'r'
			ORDER BY a.relname
		`)
	}
	return sql
}

// getColumnSQL
func getColumnSQL(tableName string) string {
	var sql string
	if dbConfig.DbType == 1 {
		sql = fmt.Sprintf(`
			select column_name as ColName,
			column_type        as ColType,
			column_key         as ColKey,
			is_nullable        as IsNullable,
			column_comment     as ColComment,
			column_default     as ColDefault
			from information_schema.columns 
			where table_schema = '%s' and table_name = '%s' order by ordinal_position
		`, dbConfig.Database, tableName)
	}
	if dbConfig.DbType == 2 {
		sql = fmt.Sprintf(`
		SELECT 
			ColName = a.name,
			ColType = b.name + '(' + cast(COLUMNPROPERTY(a.id, a.name, 'PRECISION') as varchar) + ')',
			ColKey  = case when exists(SELECT 1
										FROM sysobjects
										where xtype = 'PK'
										and name in (
											SELECT name
											FROM sysindexes
											WHERE indid in (
												SELECT indid
												FROM sysindexkeys
												WHERE id = a.id AND colid = a.colid
										))) then 'PRI'
							else '' end,
			IsNullable = case when a.isnullable = 1 then 'YES' else 'NO' end,
			ColComment = isnull(g.[value], ''),
			ColDefault = isnull(e.text, '')
		FROM syscolumns a
				left join systypes b on a.xusertype = b.xusertype
				inner join sysobjects d on a.id = d.id and d.xtype = 'U' and d.name <> 'dtproperties'
				left join syscomments e on a.cdefault = e.id
				left join sys.extended_properties g on a.id = g.major_id and a.colid = g.minor_id
				left join sys.extended_properties f on d.id = f.major_id and f.minor_id = 0
		where d.name = '%s'
		order by a.id, a.colorder
		`, tableName)
	}
	if dbConfig.DbType == 3 {
		sql = fmt.Sprintf(`
		SELECT
		COLUMN_NAME AS ColName,
		data_type AS ColType,
		CASE
				WHEN b.pk_name IS NULL THEN ''
				ELSE 'PRI'
		END AS ColKey,
		is_nullable AS IsNullable,
		pgd.description AS ColComment,
		column_default AS ColDefault
FROM
		information_schema.COLUMNS
		LEFT JOIN (
				SELECT
						pg_attribute.attname AS colname,
						pg_constraint.conname AS pk_name
				FROM
						pg_constraint
						INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
						INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
								AND pg_attribute.attnum = ANY(pg_constraint.conkey)
						INNER JOIN pg_type ON pg_type.oid = pg_attribute.atttypid
				WHERE
						pg_class.relname = '%s' -- 这里应该是你要查询的表名
						AND pg_constraint.contype = 'p'
		) b ON b.colname = information_schema.COLUMNS.COLUMN_NAME
		LEFT JOIN pg_description pgd ON pgd.objoid = ( -- 关联到pg_description
				SELECT
						pg_class.oid
				FROM
						pg_class
				WHERE
						pg_class.relname = information_schema.COLUMNS.TABLE_NAME
		) AND pgd.objsubid = information_schema.COLUMNS.ORDINAL_POSITION
WHERE
		table_schema = 'public'
		AND TABLE_NAME = '%s' -- 这里也应该是你要查询的表名
ORDER BY
		ordinal_position;`, tableName, tableName)
	}
	return sql
}
