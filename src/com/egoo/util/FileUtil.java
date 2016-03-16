package com.egoo.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.log4j.Logger;

public class FileUtil {

	
	private static final Logger log = Logger.getLogger( FileUtil.class );
	
	public FileUtil() {
		// TODO Auto-generated constructor stub
		
	}
	
	static public URL toURL( String filePath)
	{
		URL url = null;
		File file = null;
		try 
		{
			file = new File( filePath ); 
			url = file.toURL();
			return url;
		} 
		catch (MalformedURLException e) 
		{
			// TODO Auto-generated catch block
			log.info( "filePath=" + filePath + ", toURL Error!!" );
		} 
		finally
		{
			url = null;
			file = null;
		}
		return null;
	}

}
