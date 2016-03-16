package com.egoo.util;


public class DataUtil {

	static public int atoi(String str, int defaultvalue)
	{
		try
		{
			return Integer.valueOf ( str );
		}
		catch ( NumberFormatException ex )
		{
			return defaultvalue;
		}
	}
	
}
