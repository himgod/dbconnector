package com.egoo.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.log4j.Logger;



public class SerializeUtil
{
		private static final Logger log = Logger.getLogger(SerializeUtil.class);
		public static byte[] serialize(Object object)
		{
			log.info("serialize In...");
			ObjectOutputStream oos = null;
			ByteArrayOutputStream baos = null;
			try 
			{
				baos = new ByteArrayOutputStream();
				oos = new ObjectOutputStream(baos);
				oos.writeObject(object);
				return baos.toByteArray();
			}
			catch (Exception e) 
			{
				log.info("SerializeUtil.serialize error:" + e);
			}
			finally
			{
				try
				{
					baos.close();
					oos.close();
				} 
				catch (IOException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			return null;
		}
		 
		public static Object deserialize(byte[] bytes) 
		{
			log.info("deserialize In...");
			ByteArrayInputStream bais = null;
			ObjectInputStream ois = null;
			try 
			{
				//∑¥–Ú¡–ªØ
				bais = new ByteArrayInputStream(bytes);
				ois = new ObjectInputStream(bais);
				return ois.readObject();
			} 
			catch (Exception e) 
			{
				log.info("SerializeUtil.deserialize error:" + e);
			}
			finally
			{
				try
				{
					bais.close();
					ois.close();
				} 
				catch (IOException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			return null;
		}
}
