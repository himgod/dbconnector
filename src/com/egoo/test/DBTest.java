package com.egoo.test;

import com.egoo.config.DBConfigure;
import com.egoo.db.connector.DBConnectionPool;

public class DBTest {

	public DBTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args)
	{
		DBConfigure.loadConfig();
		DBConnectionPool.Start();
		
		DBConnectionPool.getDBConnector();
		
	}
}
