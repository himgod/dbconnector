package com.egoo.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil 
{
	static SimpleDateFormat sdf_yyyymmdd_hhmmssSSS = new SimpleDateFormat ( "yyyy-MM-dd HH:mm:ss.SSS" );

	static public String getDateTime()
	{
		Date now = new Date ();
		String datetime = sdf_yyyymmdd_hhmmssSSS.format ( now );
		now = null;
		return datetime;
	}

	static public String formatDate(long time)
	{
		Date now = new Date ( time );
		String datetime = sdf_yyyymmdd_hhmmssSSS.format ( now );
		now = null;
		return datetime;
	}
}
