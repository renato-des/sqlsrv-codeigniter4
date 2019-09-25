<?php 

namespace CodeIgniter\Database\SQLSRV;

use CodeIgniter\Database\BaseBuilder;

/**
 * Builder for SQLSRV
 */
class Builder extends BaseBuilder {

	/**
	 * LIMIT string
	 *
	 * Generates a platform-specific LIMIT clause.
	 *
	 * @param string $sql SQL Query
	 *
	 * @return string
	 */
	protected function _limit(string $sql): string
	{

		if (!isset( $this->QBOffset )) {
			throw new DatabaseException( 'You must use offset when supplying a limit.' );
		}

		return $sql . "OFFSET {$this->QBOffset} ROWS FETCH NEXT {$this->QBLimit} ROWS ONLY ";
	}
}
