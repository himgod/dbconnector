package com.egoo.db.connector;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Calendar;

import oracle.jdbc.driver.OracleConnection;

import org.apache.log4j.Logger;

import com.egoo.config.DBConfigure;

public class OracleConnector implements DBConnector
{
	public OracleConnection m_conn = null;

	public boolean checkConnection()
	{
		try
		{
			if ( pingDatabase () )
				return true;

			closeConnection ();
			DBConfigure.m_logger.info ( "OracleConnector.DriverManager.getConnection..." );

			String connstr = DBConfigure.dbLocal;
			int begin = connstr.indexOf ( "//" ) + 2;
			int end = connstr.indexOf ( "/", begin );
			String host = connstr.substring ( begin, end );

			begin = end + 1;
			end = connstr.indexOf ( "?", begin );
			String dbname = connstr.substring ( begin, end );

			begin = connstr.indexOf ( "user=" ) + 5;
			end = connstr.indexOf ( "&", begin );
			String username = connstr.substring ( begin, end );

			begin = connstr.indexOf ( "password=" ) + 9;
			String password = connstr.substring ( begin );

			String oracle = "jdbc:oracle:thin:@" + host + ":" + dbname;
			m_conn = (OracleConnection) DriverManager.getConnection ( oracle, username, password );

			DBConfigure.m_logger.info ( "OracleConnector.DriverManager.getConnection [ OK ]" );
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
		DBConfigure.m_logger.info ( "pingDatabase" );

		try
		{
			if ( m_conn == null || m_conn.isClosed () || m_conn.pingDatabase ( DBConfigure.INTERVAL_TO ) != OracleConnection.DATABASE_OK )
			{
				return false;
			}

			DBConfigure.m_logger.info ( "pingDatabase=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
			return true;
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString () );
		}

		return false;
	}

	public void release(ResultSet rs)
	{
		if ( rs == null )
			return;

		long start = Calendar.getInstance ().getTimeInMillis ();
		DBConfigure.m_logger.info ( "release" );

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
		DBConfigure.m_logger.info ( "release=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
	}

	public ResultSet select(String sql)
	{
		long start = Calendar.getInstance ().getTimeInMillis ();
		sql = sql.replace ( "now()", "sysdate" );
		DBConfigure.m_logger.info ( sql );

		if ( !checkConnection () )
			return null;

		Statement stmt = null;
		ResultSet rs = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.setQueryTimeout ( DBConfigure.INTERVAL_TO );
			rs = stmt.executeQuery ( sql );

			DBConfigure.m_logger.info ( "select=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
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
		sql = sql.replace ( "now()", "sysdate" );
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
		DBConfigure.m_logger.info ( sql );

		if ( !checkConnection () )
			return false;

		Statement stmt = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.setQueryTimeout ( DBConfigure.INTERVAL_TO );
			stmt.executeUpdate ( sql );

			DBConfigure.m_logger.info ( "delete=" + String.format ( "%d", Calendar.getInstance ().getTimeInMillis () - start ) );
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
		sql = sql.replace ( "now()", "sysdate" );
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
