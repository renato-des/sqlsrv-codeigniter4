<?php

namespace CodeIgniter\Database\SQLSRV;

use CodeIgniter\Database\BaseConnection;
use CodeIgniter\Database\ConnectionInterface;
use CodeIgniter\Database\Exceptions\DatabaseException;

/**
 * Connection for SQLSRV
 */
class Connection extends BaseConnection implements ConnectionInterface 
{
	/**
	 * Database driver
	 *
	 * @var string
	 */
	public $DBDriver = 'SQLSRV';
	//--------------------------------------------------------------------
	/**
	 * Identifier escape character
	 *
	 * @var string
	 */
	public $escapeChar = '"';
	//--------------------------------------------------------------------
	/**
	 * SQLSRV object
	 *
	 * Has to be preserved without being assigned to $conn_id.
	 *
	 * @var \SQLSRV
	 */
	public $sqlsrv;
	//--------------------------------------------------------------------

	/**
	 * Connect to the database.
	 *
	 * @param boolean $persistent
	 *
	 * @return mixed
	 * @throws \CodeIgniter\Database\Exceptions\DatabaseException
	 */
	public function connect(bool $persistent = false)
	{
		$charset = in_array( strtolower( $this->charset ), [
					'utf-8',
					'utf8',
				], true ) ? 'UTF-8' : SQLSRV_ENC_CHAR;

				$connection = [
					'uid'                  => empty( $this->username ) ? '' : $this->username,
					'pwd'                  => empty( $this->password ) ? '' : $this->password,
					'database'             => $this->database,
					'CharacterSet'         => $charset,
					'Encrypt'              => ( $this->encrypt === true ) ? 1 : 0,
					'ReturnDatesAsStrings' => 1,
				];
				// If the username and password are both empty, assume this is a
				// 'Windows Authentication Mode' connection.
				if (empty( $connection['uid'] ) && empty( $connection['pwd'] )) {
					unset( $connection['uid'], $connection['pwd'] );
				}

				if (false !== ( $this->connID = sqlsrv_connect( $this->hostname, $connection ) )) {

					$query = $this->query( 'SELECT CASE WHEN (@@OPTIONS | 256) = @@OPTIONS THEN 1 ELSE 0 END AS qi' );
					$query = $query->getRowArray();
					$this->quotedIdentifier = empty( $query ) ? false : (bool) $query['qi'];

					$this->escapeChar = ( $this->quotedIdentifier ) ? '"' : [
						'[',
						']',
					];
				}

				return $this->connID;
	}

	//--------------------------------------------------------------------

	/**
	 * Select a specific database table to use.
	 *
	 * @param string $databaseName
	 *
	 * @return boolean
	 */
	public function setDatabase(string $databaseName): bool
	{
		return false;
	}

	//--------------------------------------------------------------------

	/**
	 * Returns a string containing the version of the database being used.
	 *
	 * @return string
	 */
	public function getVersion(): string
	{
		if (isset( $this->dataCache['version'] )) {
			return $this->dataCache['version'];
		}

		if (!$this->connID OR ( $info = sqlsrv_server_info( $this->connID ) ) === false) {
			return false;
		}

		return $this->dataCache['version'] = $info['SQLServerVersion'];
	}

	//--------------------------------------------------------------------

	/**
	 * Executes the query against the database.
	 *
	 * @param string $sql
	 *
	 * @return mixed
	 */
	public function execute(string $sql)
	{
		return sqlsrv_query( $this->connID, $sql );
	}

	//--------------------------------------------------------------------

	/**
	 * Begin Transaction
	 *
	 * @return boolean
	 */
	protected function _transBegin(): bool
	{
		return sqlsrv_begin_transaction( $this->connID );
	}

	//--------------------------------------------------------------------

	/**
	 * Commit Transaction
	 *
	 * @return    bool
	 */
	protected function _transCommit() : bool 
	{
		return sqlsrv_commit( $this->connID );
	}

	//--------------------------------------------------------------------

	/**
	 * Rollback Transaction
	 *
	 * @return    bool
	 */
	protected function _transRollback() : bool 
	{
		return sqlsrv_rollback( $this->connID );
	}

	//--------------------------------------------------------------------

	/**
	 * Returns the total number of rows affected by this query.
	 *
	 * @return mixed
	 */
	public function affectedRows() : int 
	{
		return sqlsrv_rows_affected( $this->resultID );
	}

	//--------------------------------------------------------------------

	/**
	 * Returns the last error code and message.
	 *
	 * Must return an array with keys 'code' and 'message':
	 *
	 *  return ['code' => null, 'message' => null);
	 *
	 * @return    array
	 */
	public function error(): array
	{
		$error = [
			'code'    => '00000',
			'message' => '',
		];

		$sqlsrvErrors = sqlsrv_errors( SQLSRV_ERR_ALL );

		if (!is_array( $sqlsrvErrors )) {
			return $error;
		}

		$sqlsrvError = array_shift( $sqlsrvErrors );

		if (isset( $sqlsrvError['SQLSTATE'] )) {
			$error['code'] = isset( $sqlsrvError['code'] ) ? $sqlsrvError['SQLSTATE'] . '/' . $sqlsrvError['code'] : $sqlsrvError['SQLSTATE'];
		} else if (isset( $sqlsrvError['code'] )) {
			$error['code'] = $sqlsrvError['code'];
		}

		if (isset( $sqlsrvError['message'] )) {
			$error['message'] = $sqlsrvError['message'];
		}

		return $error;
	}

	//--------------------------------------------------------------------

	/**
	 * Insert ID
	 *
	 * @return    int
	 */
	public function insertID(): int
	{
		return $this->query( 'SELECT SCOPE_IDENTITY() AS insertID' )->row()->insertID;
	}

	//--------------------------------------------------------------------

	/**
	 * Generates the SQL for listing tables in a platform-dependent manner.
	 *
	 * @param boolean $prefixLimit
	 *
	 * @return string
	 */
	protected function _listTables($prefixLimit = false): string 
	{
		$sql = 'SELECT ' . $this->escapeIdentifiers( 'name' ) . ' FROM ' . $this->escapeIdentifiers( 'sysobjects' ) . ' WHERE ' . $this->escapeIdentifiers( 'type' ) . " = 'U'";

		if ($prefixLimit === true && $this->DBPrefix !== '') {
			$sql .= ' AND ' . $this->escapeIdentifiers( 'name' ) . " LIKE '" . $this->escapeLikeString( $this->DBPrefix ) . "%' " . sprintf( $this->likeEscapeStr, $this->likeEscapeChar );
		}

		return $sql . ' ORDER BY ' . $this->escapeIdentifiers( 'name' );
	}

	//--------------------------------------------------------------------

	/**
	 * Generates a platform-specific query string so that the column names can be fetched.
	 *
	 * @param string $table
	 *
	 * @return string
	 */
	protected function _listColumns(string $table = ''): string 
	{
		return 'SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_NAME) = ' . $this->escape( strtoupper( $this->DBPrefix . $table ) );
	}

	//--------------------------------------------------------------------

	/**
	 * Returns an array of objects with field data
	 *
	 * @param  string $table
	 * @return \stdClass[]
	 * @throws DatabaseException
	 */
	public function _fieldData(string $table): array
	{
		$sql = "SELECT  c.TABLE_NAME, c.COLUMN_NAME,c.DATA_TYPE, c.COLUMN_DEFAULT, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION
             ,CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS KeyType
			FROM INFORMATION_SCHEMA.COLUMNS c
			LEFT JOIN (
						SELECT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME
						FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
						INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
							ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
							AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
					)   pk 
			ON  c.TABLE_CATALOG = pk.TABLE_CATALOG
						AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
						AND c.TABLE_NAME = pk.TABLE_NAME
						AND c.COLUMN_NAME = pk.COLUMN_NAME
			WHERE UPPER(c.TABLE_NAME) =  " . $this->escape(strtoupper($table));

		if (($query = $this->query($sql)) === FALSE)
		{
			throw new DatabaseException(lang('Database.failGetFieldData'));
		}
		$query = $query->getResultObject();
		
		$retval = [];
		for ($i = 0, $c = count($query); $i < $c; $i++)
		{
			$retval[$i]			= new \stdClass();
			$retval[$i]->name		= $query[$i]->COLUMN_NAME;
			$retval[$i]->type		= $query[$i]->DATA_TYPE;
			$retval[$i]->max_length		= ($query[$i]->CHARACTER_MAXIMUM_LENGTH > 0) ? $query[$i]->CHARACTER_MAXIMUM_LENGTH : $query[$i]->NUMERIC_PRECISION;
			$retval[$i]->default		= $query[$i]->COLUMN_DEFAULT;
			$retval[$i]->primary_key = $query[$i]->KeyType;
		}
		return $retval;
	}

	//--------------------------------------------------------------------

	/**
	 * Returns an array of objects with index data
	 *
	 * @param  string $table
	 * @return \stdClass[]
	 * @throws DatabaseException
	 * @throws \LogicException
	 */
	public function _indexData(string $table): array
	{
		$sql = "SELECT
					CASE WHEN i.is_primary_key = 1 THEN 'PRIMARY'
							WHEN i.is_unique = 1 THEN 'UNIQUE'
							ELSE i.name END AS [Key_name],
					CASE WHEN ic.column_id = 1 THEN 'PRIMARY'
							WHEN ic.column_id <> 1 THEN 'INDEX' END AS [Type],
					c.name
					FROM  sys.indexes i 
					INNER JOIN 	sys.index_columns ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id
					INNER JOIN sys.columns c ON ic.column_id = c.column_id AND ic.object_id = c.object_id
					WHERE OBJECT_NAME(i.object_id) = " . $this->escape($table);

		if (($query = $this->query($sql)) === false)
		{
			throw new DatabaseException(lang('Database.failGetIndexData'));
		}
		$query = $query->getResultObject();

		$retval = [];
		foreach ($query as $row)
		{
			$obj = new \stdClass();
			$obj->name = $row->Key_name;
			$obj->type = $row->Type;

			$retval[$obj->name] = $obj;
			$retval[$obj->name]->fields[] = $row->name;
		}

		return $retval;
	}

	//--------------------------------------------------------------------

	/**
	 * Returns an array of objects with Foreign key data
	 *
	 * @param  string $table
	 * @return \stdClass[]
	 * @throws DatabaseException
	 */
	public function _foreignKeyData(string $table): array
	{
	/**
	 * Columns
	 * foreign_table - foreign table name with schema name
	 * primary_table - primary (rerefenced) table name with schema name
	 * fk_constraint_name - foreign key constraint name
	 * 
	 * Rows
	 * One row represents one foreign key. If foreign key consists of multiple columns (composite key) it is still represented as one row.
	 * Scope of rows: all foregin keys in a database
	 */

		$sql = "SELECT schema_name(fk_tab.schema_id) + '.' + fk_tab.name AS foreign_table,
						schema_name(pk_tab.schema_id) + '.' + pk_tab.name AS primary_table,
						fk.name AS fk_constraint_name
					FROM sys.foreign_keys fk
						INNER JOIN sys.tables fk_tab ON fk_tab.object_id = fk.parent_object_id
						INNER JOIN sys.tables pk_tab ON pk_tab.object_id = fk.referenced_object_id
						CROSS APPLY ( SELECT col.[name] + ', '
									FROM sys.foreign_key_columns fk_c
										INNER JOIN sys.columns col ON fk_c.parent_object_id = col.object_id AND fk_c.parent_column_id = col.column_id
									WHERE fk_c.parent_object_id = fk_tab.object_id
										AND fk_c.constraint_object_id = fk.object_id
										AND fk_tab.name = " . $this->escape($table) . "
						FOR XML PATH ('') ) D (column_names)";

		if (($query = $this->query($sql)) === false)
		{
			throw new DatabaseException(lang('Database.failGetForeignKeyData'));
		}
		$query = $query->getResultObject();

		$retval = [];
		foreach ($query as $row)
		{
			$obj = new \stdClass();
			$obj->constraint_name = $row->fk_constraint_name;
			$obj->table_name = $row->primary_table;
			$obj->foreign_table_name = $row->foreign_table;

			$retval[] = $obj;
		}

		return $retval;
	}

	//--------------------------------------------------------------------

	/**
	 * Keep or establish the connection if no queries have been sent for
	 * a length of time exceeding the server's idle timeout.
	 *
	 * @return void
	 */
	public function reconnect() {
		$this->close();
		$this->initialize();
	}

	//--------------------------------------------------------------------

	/**
	 * Close the database connection.
	 *
	 * @return void
	 */
	protected function _close() {
		sqlsrv_close( $this->connID );
	}

	//--------------------------------------------------------------------


}