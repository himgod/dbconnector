package com.egoo.db.connector;

import org.apache.commons.lang.math.RandomUtils;

import com.egoo.config.DBConfigure;

public class DBConnectionPool
{
	static private DBConnector[] m_dbConnectors = null;

	static public void Start()
	{
		try 
		{
			Class.forName ( "com.mysql.jdbc.Driver" );
			Class.forName ( "oracle.jdbc.driver.OracleDriver" );
			
			if ( DBConfigure.dbLocal.contains ( "oracle:" ) )
			{
				m_dbConnectors = new OracleConnector[DBConfigure.DBPOOL_SIZE];

				for ( int i = 0; i < DBConfigure.DBPOOL_SIZE; ++i )
				{
					m_dbConnectors[i] = new OracleConnector ();
					m_dbConnectors[i].checkConnection ();
				}
			}
			else
			{
				m_dbConnectors = new MySQLConnector[DBConfigure.DBPOOL_SIZE];

				for ( int i = 0; i < DBConfigure.DBPOOL_SIZE; ++i )
				{
					m_dbConnectors[i] = new MySQLConnector ();
					m_dbConnectors[i].checkConnection ();
				}
			}
		} 
		catch (ClassNotFoundException e) 
		{
			// TODO Auto-generated catch block
			DBConfigure.m_logger.error ( e.toString (), e );
		}
		
		
		
	}

	static public void checkConnection()
	{
		for ( int i = 0; i < DBConfigure.DBPOOL_SIZE; ++i )
		{
			m_dbConnectors[i].checkConnection ();
		}
	}

	static public DBConnector getDBConnector()
	{
		int i = RandomUtils.nextInt ( DBConfigure.DBPOOL_SIZE );
		return m_dbConnectors[i];
	}
}
