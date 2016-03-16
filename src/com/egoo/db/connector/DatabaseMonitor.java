package com.egoo.db.connector;

import com.egoo.config.DBConfigure;

class DatabaseCheckThread extends Thread
{
	
	public void run()
	{
		while ( true )
		{
			try
			{
				DBConnectionPool.checkConnection ();
				sleep ( DBConfigure.INTERVAL_DC * 1000 );
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
			}
		}
	}
}

public class DatabaseMonitor
{
	static private DatabaseCheckThread m_DatabaseCheckThread;

	static public void Start()
	{
		try
		{
			m_DatabaseCheckThread = new DatabaseCheckThread ();
			m_DatabaseCheckThread.start ();
		}
		catch ( Exception ex )
		{
			DBConfigure.m_logger.error ( ex.toString (), ex );
		}
	}
}
