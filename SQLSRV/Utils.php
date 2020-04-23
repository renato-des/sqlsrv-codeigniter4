<?php

namespace CodeIgniter\Database\SQLSRV;

/**
 * Utils for SQLSRV
 */
class Utils extends \CodeIgniter\Database\BaseUtils
{

	/**
	 * List databases statement
	 *
	 * @var string
	 */
	protected $listDatabases = 'SELECT name FROM master.sys.databases';

	/**
	 * OPTIMIZE TABLE statement
	 *
	 * @var string
	 */
	protected $optimizeTable = 'ALTER INDEX ALL ON %s REBUILD';

	//--------------------------------------------------------------------

	/**
	 * Platform dependent version of the backup function.
	 *
	 * @param array|null $prefs
	 *
	 * @return mixed
	 */
	public function _backup(array $prefs = null)
	{
		throw new DatabaseException('Unsupported feature of the database platform you are using.');
	}
	//--------------------------------------------------------------------
}
