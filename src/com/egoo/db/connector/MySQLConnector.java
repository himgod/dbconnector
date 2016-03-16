package com.egoo.db.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Calendar;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.egoo.config.DBConfigure;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class MySQLConnector implements DBConnector
{
	public Connection m_conn = null;
//	public PropertyConfigurator.configure ( "log4j.properties" );
	
	public boolean checkConnection()
	{
		try
		{
			if ( pingDatabase () )
				return true;

			closeConnection ();
			DBConfigure.m_logger.info ( "MySQLConnector.DriverManager.getConnection..." );
			m_conn = DriverManager.getConnection ( "jdbc:" + DBConfigure.dbLocal );
			DBConfigure.m_logger.info ( "MySQLConnector.DriverManager.getConnection [ OK ]" );

			return pingDatabase ();
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
			return false;
		}
	}

	public boolean pingDatabase()
	{
		long start = Calendar.getInstance ().getTimeInMillis ();
		DBConfigure.m_logger.debug ( "pingDatabase" );
		Statement stmt = null;

		try
		{
			if ( m_conn == null || m_conn.isClosed () )
			{
				return false;
			}

			stmt = m_conn.createStatement ();
			stmt.setQueryTimeout ( DBConfigure.INTERVAL_TO );
			stmt.executeUpdate ( "delete from t_connection_check" );

			DBConfigure.m_logger.debug ( "pingDatabase=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
			return true;
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
			return false;
		}
		finally
		{
			closeStatement ( stmt );
		}
	}

	public void release(ResultSet rs)
	{
		if ( rs == null )
			return;

		long start = Calendar.getInstance ().getTimeInMillis ();
		DBConfigure.m_logger.debug ( "release" );

		try
		{
			rs.getStatement ().close ();
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
		}

		try
		{
			rs.close ();
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
		}

		rs = null;
		DBConfigure.m_logger.debug ( "release=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
	}

	public ResultSet select(String sql)
	{
		long start = Calendar.getInstance ().getTimeInMillis ();
		DBConfigure.m_logger.debug ( sql );

		if ( !checkConnection () )
			return null;

		Statement stmt = null;
		ResultSet rs = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.setQueryTimeout ( DBConfigure.INTERVAL_TO );
			rs = stmt.executeQuery ( sql );

			DBConfigure.m_logger.debug ( "select=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
			return rs;
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );

			try
			{
				if ( rs != null )
				{
					rs.close ();
					rs = null;
				}
			}
			catch ( Exception ex2 )
			{
				DBConfigure.m_logger.error ( ex2.toString () );
			}

			try
			{
				if ( stmt != null )
				{
					stmt.close ();
					stmt = null;
				}
			}
			catch ( Exception ex2 )
			{
				DBConfigure.m_logger.error ( ex2.toString () );
			}

			return null;
		}
	}

	public boolean insert(String sql)
	{
		long start = Calendar.getInstance ().getTimeInMillis ();
		DBConfigure.m_logger.info ( sql );

		if ( !checkConnection () )
			return false;

		Statement stmt = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.setQueryTimeout ( DBConfigure.INTERVAL_TO );
			stmt.executeUpdate ( sql );

			DBConfigure.m_logger.info ( "insert=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
			return true;
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
			return false;
		}
		finally
		{
			closeStatement ( stmt );
		}
	}

	public boolean delete(String sql)
	{
		long start = Calendar.getInstance ().getTimeInMillis ();
		DBConfigure.m_logger.debug ( sql );

		if ( !checkConnection () )
			return false;

		Statement stmt = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.setQueryTimeout ( DBConfigure.INTERVAL_TO );
			stmt.executeUpdate ( sql );

			DBConfigure.m_logger.debug ( "delete=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
			return true;
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
			return false;
		}
		finally
		{
			closeStatement ( stmt );
		}
	}

	public boolean update(String sql)
	{
		long start = Calendar.getInstance ().getTimeInMillis ();
		DBConfigure.m_logger.info ( sql );

		if ( !checkConnection () )
			return false;

		Statement stmt = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.setQueryTimeout ( DBConfigure.INTERVAL_TO );
			stmt.executeUpdate ( sql );

			DBConfigure.m_logger.info ( "update=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
			return true;
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
			return false;
		}
		finally
		{
			closeStatement ( stmt );
		}
	}

	public void closeStatement(Statement stmt)
	{
		try
		{
			if ( stmt != null )
			{
				stmt.close ();
			}
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
			closeConnection ();
		}
		finally
		{
			stmt = null;
		}
	}

	public void closeConnection()
	{
		try
		{
			if ( m_conn != null )
			{
				m_conn.close ();
			}
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
		}
		finally
		{
			m_conn = null;
		}
	}
}
