<?php 

namespace CodeIgniter\Database\SQLSRV;

use CodeIgniter\Database\BaseResult;
use CodeIgniter\Database\ResultInterface;

/**
 * Result for SQLSRV
 */
class Result extends BaseResult implements ResultInterface {
	/**
	 * Gets the number of fields in the result set.
	 *
	 * @return int
	 */
	public function getFieldCount() : int 
	{
		return sqlsrv_num_fields( $this->resultID );
	}

	/**
	 * Generates an array of column names in the result set.
	 *
	 * @return array
	 */
	public function getFieldNames() : array 
	{
		$fieldNames = [];
		foreach (sqlsrv_field_metadata( $this->resultID ) as $offset => $field) {
			$fieldNames[] = $field['Name'];
		}

		return $fieldNames;
	}

	/**
	 * Generates an array of objects representing field meta-data.
	 *
	 * @return array
	 */
	public function getFieldData() : array 
	{
		$retval = [];
		foreach (sqlsrv_field_metadata( $this->resultID ) as $i => $field) {
			$retval[ $i ] = new stdClass();
			$retval[ $i ]->name = $field['Name'];
			$retval[ $i ]->type = $field['Type'];
			$retval[ $i ]->max_length = $field['Size'];
		}

		return $retval;
	}

	/**
	 * Frees the current result.
	 *
	 * @return mixed
	 */
	public function freeResult() 
	{
		if (is_resource( $this->resultID )) {
			sqlsrv_free_stmt( $this->resultID );
			$this->resultID = false;
		}
	}

	/**
	 * Moves the internal pointer to the desired offset. This is called
	 * internally before fetching results to make sure the result set
	 * starts at zero.
	 *
	 * @param int $n
	 *
	 * @return array|false|mixed|null
	 */
	public function dataSeek(int $n = 0) 
	{
		return sqlsrv_fetch( $this->resultID, $n );

	}

	/**
	 * Returns the result set as an array.
	 *
	 * Overridden by driver classes.
	 *
	 * @return array
	 */
	protected function fetchAssoc() 
	{
		return sqlsrv_fetch_array( $this->resultID, SQLSRV_FETCH_ASSOC );

	}

	/**
	 * Returns the result set as an object.
	 *
	 * Overridden by child classes.
	 *
	 * @param string $className
	 *
	 * @return object
	 */
	protected function fetchObject(string $className = 'stdClass') 
	{
		return sqlsrv_fetch_object( $this->resultID, $className );

	}
}
