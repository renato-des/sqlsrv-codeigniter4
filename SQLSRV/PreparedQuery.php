<?php 

namespace CodeIgniter\Database\SQLSRV;

use CodeIgniter\Database\BasePreparedQuery;
use CodeIgniter\Database\PreparedQueryInterface;
// use CodeIgniter\Database\ResultInterface;

class PreparedQuery extends BasePreparedQuery implements PreparedQueryInterface {
	/**
	 * The database-dependent portion of the prepare statement.
	 *
	 * @param string $sql
	 * @param array  $options Passed to the connection's prepare statement.
	 *
	 * @return mixed
	 */
	public function _prepare(string $sql, array $options = []) {

		if (!$this->statement = sqlsrv_prepare( $this->connID, $sql, $options )) {
			//TODO: Error stuff here
		}

		return $this;
	}

	/**
	 * The database dependant version of the execute method.
	 *
	 * @param array $data
	 *
	 * @return ResultInterface
	 */
	public function _execute(array $data): bool 
	{
		sqlsrv_execute($data);
		return ( $this->scrollable === false OR $this->isWriteType( $data ) ) ? sqlsrv_query( $this->conn_id, $sql ) : sqlsrv_query( $this->conn_id, $sql, null, ['Scrollable' => $this->scrollable] );

	}

	/**
	 * Returns the result object for the prepared query.
	 *
	 * @return mixed
	 */
	public function _getResult() {
		parent::_getResult();
	}
}
