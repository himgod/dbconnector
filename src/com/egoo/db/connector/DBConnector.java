package com.egoo.db.connector;

import java.sql.ResultSet;
import java.sql.Statement;

public interface DBConnector
{
	public boolean checkConnection();

	public boolean pingDatabase();

	public void release(ResultSet rs);

	public ResultSet select(String sql);

	public boolean insert(String sql);

	public boolean delete(String sql);

	public boolean update(String sql);

	public void closeStatement(Statement stmt);

	public void closeConnection();
}
