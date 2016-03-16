package com.egoo.config;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


import Albert.CfgFileReader.CfgFileReader;

public class DBConfigure {

	public static String dbLocal = null;
	// [General]
	static public Logger m_logger;
	static public int INTERVAL_GC;
	static public int INTERVAL_DC;
	static public int INTERVAL_SR;
	static public int INTERVAL_TO;
	static public int DBPOOL_SIZE;
	static public int UDP_POOL_SIZE;
	
	public DBConfigure() {
		// TODO Auto-generated constructor stub
	}
	
	public static void loadConfig()
	{
		
		CfgFileReader cfg = null;

		try
		{
			InitLog();
			cfg = new CfgFileReader ( "dao.ini" );
			//[General]
			INTERVAL_GC = cfg.getInteger ( "INTERVAL_GC", 60 );
			INTERVAL_DC = cfg.getInteger ( "INTERVAL_DC", 60 );
			INTERVAL_SR = cfg.getInteger ( "INTERVAL_SR", 60 );
			INTERVAL_TO = cfg.getInteger ( "INTERVAL_TO", 1 );
			DBPOOL_SIZE = cfg.getInteger ( "DBPOOL_SIZE", 10 );
			UDP_POOL_SIZE = cfg.getInteger ( "UDP_POOL_SIZE", 10 );
			// [Database]
			dbLocal = cfg.getString ( "DATABASE", "mysql://127.0.0.1:3306/xsvr?user=xsvr&password=xsvr1234" );

		}
		catch ( Exception ex )
		{
			ex.printStackTrace ();
		}
		finally
		{
			if ( cfg != null )
			{
				cfg.close ();
			}
		}
	}
	
	static private void InitLog()
	{
		try
		{
			m_logger = Logger.getLogger ( DBConfigure.class.getName () );
			PropertyConfigurator.configure ( "log4j.properties" );
		}
		catch ( Exception ex )
		{
			ex.printStackTrace ();
		}
	}

}
