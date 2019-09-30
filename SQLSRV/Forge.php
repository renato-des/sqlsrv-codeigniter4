<?php 

namespace CodeIgniter\Database\SQLSRV;

use CodeIgniter\Database\Exceptions\DatabaseException;

/**
 * Forge for SQLSRV
 */
class Forge extends \CodeIgniter\Database\Forge
{
	/**
	 * CREATE DATABASE statement
	 *
	 * @var    string
	 */
	protected $createDatabaseStr = 'CREATE DATABASE %s COLLATE %s';

	//--------------------------------------------------------------------

	/**
	 * CREATE TABLE IF statement
	 *
	 * @var string
	 */
	protected $createTableIfStr = "IF NOT EXISTS (SELECT * FROM sysobjects WHERE ID = object_id(N'%s') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)\nCREATE TABLE";

	//--------------------------------------------------------------------

	/**
	 * DROP TABLE IF EXISTS statement
	 *
	 * @var string
	 */
	protected $dropTableIfStr = "IF EXISTS (SELECT * FROM sysobjects WHERE ID = object_id(N'%s') AND OBJECTPROPERTY(id, N'IsUserTable') = 1)\nDROP TABLE";

	//--------------------------------------------------------------------

	/**
	 * Create database
	 *
	 * @param string $db_name
	 *
	 * @return boolean
	 * @throws \CodeIgniter\Database\Exceptions\DatabaseException
	 */
	public function createDatabase($db_name): bool
	{
		if ($this->createDatabaseStr === false)
		{
			if ($this->db->DBDebug)
			{
				throw new DatabaseException('This feature is not available for the database you are using.');
			}

			return false;
		}
		elseif (! $this->db->query(sprintf($this->createDatabaseStr, $db_name, $this->db->DBCollat))
		)
		{
			if ($this->db->DBDebug)
			{
				throw new DatabaseException('Unable to drop the specified database.');
			}

			return false;
		}

		if (! empty($this->db->dataCache['db_names']))
		{
			$this->db->dataCache['db_names'][] = $db_name;
		}

		return true;
	}


	/**
	 * CREATE TABLE keys flag
	 *
	 * Whether table keys are created from within the
	 * CREATE TABLE statement.
	 *
	 * @var    bool
	 */
	protected $createTableKeys = true;

	/**
	 * UNSIGNED support
	 *
	 * @var	array
	 */
	protected $unsigned		= array(
		'TINYINT'	=> 'SMALLINT',
		'SMALLINT'	=> 'INT',
		'INT'		=> 'BIGINT',
		'REAL'		=> 'FLOAT',
	);

	/**
	 * NULL value representation in CREATE/ALTER TABLE statements
	 *
	 * @var    string
	 */
	protected $_null = 'NULL';

	//--------------------------------------------------------------------

	/**
	 * CREATE TABLE attributes
	 *
	 * @param	array	$attributes	Associative array of table attributes
	 * @return	string
	 */
	protected function _createTableAttributes(array $attributes): string
	{
		$sql = '';

		foreach (array_keys($attributes) as $key)
		{
			if (is_string($key))
			{
				$sql .= ' '.strtoupper($key).' = '.$attributes[$key];
			}
		}

		if ( ! empty($this->db->charset) && ! strpos($sql, 'CHARACTER SET') && ! strpos($sql, 'CHARSET'))
		{
			$sql .= ' DEFAULT CHARACTER SET = '.$this->db->charset;
		}

		if ( ! empty($this->db->DBCollat) && ! strpos($sql, 'COLLATE'))
		{
			$sql .= ' COLLATE = '.$this->db->DBCollat;
		}

		return $sql;
	}

	//--------------------------------------------------------------------

	/**
	 * ALTER TABLE
	 *
	 * @param	string	$alter_type	ALTER type
	 * @param	string	$table		Table name
	 * @param	mixed	$field		Column definition
	 * @return	string|string[]
	 */
	protected function _alterTable(string $alter_type, string $table, $field)
	{
		if ($alter_type === 'DROP')
		{
			return parent::_alterTable($alter_type, $table, $field);
		}

		$sql = 'ALTER TABLE '.$this->db->escapeIdentifiers($table);
		for ($i = 0, $c = count($field); $i < $c; $i++)
		{
			if ($field[$i]['_literal'] !== FALSE)
			{
				$field[$i] = ($alter_type === 'ADD')
					? "\n\tADD ".$field[$i]['_literal']
					: "\n\tMODIFY ".$field[$i]['_literal'];
			}
			else
			{
				if ($alter_type === 'ADD')
				{
					$field[$i]['_literal'] = "\n\tADD ";
				}
				else
				{
					$field[$i]['_literal'] = empty($field[$i]['new_name']) ? "\n\tMODIFY " : "\n\tCHANGE ";
				}

				$field[$i] = $field[$i]['_literal'].$this->_processColumn($field[$i]);
			}
		}

		return array($sql.implode(',', $field));
	}

	//--------------------------------------------------------------------

	/**
	 * Process column
	 *
	 * @param	array	$field
	 * @return	string
	 */
	protected function _processColumn(array $field): string
	{
		$extra_clause = isset($field['after'])
			? ' AFTER '.$this->db->escapeIdentifiers($field['after']) : '';

		if (empty($extra_clause) && isset($field['first']) && $field['first'] === TRUE)
		{
			$extra_clause = ' FIRST';
		}

		return $this->db->escapeIdentifiers($field['name'])
		       .(empty($field['new_name']) ? '' : ' '.$this->db->escapeIdentifiers($field['new_name']))
		       .' '.$field['type'].$field['length']
		       .$field['null']
		       .$field['default']
		       .$field['auto_increment']
		       .$field['unique']
		       .(empty($field['comment']) ? '' : ' COMMENT '.$field['comment'])
		       .$extra_clause;
	}

	//--------------------------------------------------------------------

	/**
	 * Process indexes
	 *
	 * @param	string	$table	(ignored)
	 * @return	string
	 */
	protected function _processIndexes(string $table)
	{
		$sql = '';

		for ($i = 0, $c = count($this->keys); $i < $c; $i++)
		{
			if (is_array($this->keys[$i]))
			{
				for ($i2 = 0, $c2 = count($this->keys[$i]); $i2 < $c2; $i2++)
				{
					if ( ! isset($this->fields[$this->keys[$i][$i2]]))
					{
						unset($this->keys[$i][$i2]);
						continue;
					}
				}
			}
			elseif ( ! isset($this->fields[$this->keys[$i]]))
			{
				unset($this->keys[$i]);
				continue;
			}

			is_array($this->keys[$i]) OR $this->keys[$i] = array($this->keys[$i]);

			$sql .= ",\n\tKEY ".$this->db->escapeIdentifiers(implode('_', $this->keys[$i]))
			        .' ('.implode(', ', $this->db->escapeIdentifiers($this->keys[$i])).')';
		}

		$this->keys = array();

		return $sql;
	}

	//--------------------------------------------------------------------

	/**
	 * Field attribute AUTO_INCREMENT
	 *
	 * @param array &$attributes
	 * @param array &$field
	 *
	 * @return void
	 */
	protected function _attributeAutoIncrement(array &$attributes, array &$field)
	{
		if ( ! empty($attributes['AUTO_INCREMENT']) && $attributes['AUTO_INCREMENT'] === TRUE && stripos($field['type'], 'int') !== FALSE)
		{
			$field['auto_increment'] = ' IDENTITY(1,1)';
		}
	}

	//--------------------------------------------------------------------

	/**
	 * Field attribute TYPE
	 *
	 * Performs a data type mapping between different databases.
	 *
	 * @param array &$attributes
	 *
	 * @return void
	 */
	protected function _attributeType(array &$attributes)
	{
		if (isset($attributes['CONSTRAINT']) && strpos($attributes['TYPE'], 'INT') !== FALSE)
		{
			unset($attributes['CONSTRAINT']);
		}

		switch (strtoupper($attributes['TYPE']))
		{
			case 'MEDIUMINT':
				$attributes['TYPE'] = 'INTEGER';
				$attributes['UNSIGNED'] = FALSE;
				break;
			case 'INTEGER':
				$attributes['TYPE'] = 'INT';
				break;
			default:
				break;
		}
	}


}
