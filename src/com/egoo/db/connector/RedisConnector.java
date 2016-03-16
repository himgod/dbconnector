package com.egoo.db.connector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import com.egoo.config.DBConfigure;
import com.egoo.util.SerializeUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import Albert.CfgFileReader.CfgFileReader;


public class RedisConnector
{
	static public HashSet<String> REDIS_SERVER = new HashSet<String> ();
	static public String REDIS_MASTER = "master";
	static public String REDIS_PASSWORD = "";
	static public int REDIS_TIME_OUT = 1000;
	static public int REDIS_DATABASE = 0;
	static private JedisSentinelPool jedisSentinelPool = null;
	static private Object syncObj = new Object ();

	static public void Start()
	{
		synchronized ( syncObj )
		{
			CfgFileReader cfg = null;

			try
			{
				cfg = new CfgFileReader ( "dao.ini" );

				// [Redis]
				REDIS_MASTER = cfg.getString ( "REDIS_MASTER", "master" );
				REDIS_PASSWORD = cfg.getString ( "REDIS_PASSWORD", "master" );
				REDIS_TIME_OUT = cfg.getInteger ( "REDIS_TIME_OUT", 4000 );
				REDIS_DATABASE = cfg.getInteger ( "REDIS_DATABASE", 0 );
				if ( REDIS_PASSWORD.length () == 0 )
					REDIS_PASSWORD = null;

				for ( int i = 0; i < 10; ++i )
				{
					String key = String.format ( "REDIS_SERVER_%d", i + 1 );
					String value = cfg.getString ( key, "" );
					value = value.replace ( "tcp://", "" );
					value = value.replace ( "/", "" );
					value = value.trim ();

					if ( value.length () > 0 )
					{
						REDIS_SERVER.add ( value );
					}
				}

				if ( jedisSentinelPool != null )
				{
					jedisSentinelPool.destroy ();
					jedisSentinelPool = null;
				}

				DBConfigure.m_logger.info ( "Creating JedisSentinelPool..." );
				DBConfigure.m_logger.info("master:"+REDIS_MASTER);
				DBConfigure.m_logger.info("server:"+REDIS_SERVER);
				DBConfigure.m_logger.info("timeout:"+REDIS_TIME_OUT);
				DBConfigure.m_logger.info("password:"+REDIS_PASSWORD);
				DBConfigure.m_logger.info("database:"+REDIS_DATABASE);
				jedisSentinelPool = new JedisSentinelPool ( REDIS_MASTER, REDIS_SERVER, new GenericObjectPoolConfig (), REDIS_TIME_OUT, REDIS_PASSWORD, REDIS_DATABASE );
				DBConfigure.m_logger.info ( "Succ" );
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
			}
			finally
			{
				if ( cfg != null )
				{
					cfg.close ();
					cfg = null;
				}
			}
		}
	}

	static public boolean Set(String key, String value)
	{
		if ( jedisSentinelPool == null )
			return false;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					if ( value == null )
						value = "";

					DBConfigure.m_logger.info ( "Set " + key + " = " + value );
					jedis.set ( key, value );
					return true;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return false;
		}
	}

	static public boolean SetObject(String key, Object value)
	{
		if ( jedisSentinelPool == null )
			return false;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					if ( value == null )
						value = "";

					DBConfigure.m_logger.info ( "Set " + key + " = " + value );
					jedis.set( key.getBytes() , SerializeUtil.serialize(value));
					return true;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return false;
		}
	}
	static public String Get(String key, String defaultval)
	{
		if ( jedisSentinelPool == null )
			return defaultval;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					String value = jedis.get ( key );

					if ( value == null )
						value = defaultval;

					DBConfigure.m_logger.info ( "Get " + key + " = " + value );
					return value;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return defaultval;
		}
	}


	static public boolean Del(String key)
	{
		if ( jedisSentinelPool == null )
			return false;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					DBConfigure.m_logger.info ( "Del " + key );
					if ( jedis.exists(key) )
					{
						jedis.del ( key );
						return true;
					}
					return false;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return false;
		}
	}

	static public Set<String> Select(String pattern)
	{
		if ( jedisSentinelPool == null )
			return null;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedis != null )
				{
					jedis.set ( "1", "1" );
					Set<String> keys = jedis.keys ( pattern );
					DBConfigure.m_logger.info ( "Select " + pattern + " = " + keys );
					return keys;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return null;
		}
	}

	//by himgod 2015.12.16
	static public boolean ldel(String key,String memberid)
	{
		if ( jedisSentinelPool == null )
			return false;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					List<String> list = jedis.lrange(key, 0, -1); 
					List<String> tmpList = new ArrayList<String>();
					for( String tmpMemberId : list )
					{
						if( !tmpMemberId.equals(memberid) )
						{
							tmpList.add(tmpMemberId);
						}
					}
					RedisConnector.Del(key);
					for( String tmpMemberId:tmpList )
					{
						RedisConnector.lpush(key, tmpMemberId);
					}
					
					
					DBConfigure.m_logger.info ( "List.ldel " + key + " ,memberid=" + memberid + ",last list.size()=" + list.size());
					return true;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return false;
		}
	}
	//by himgod 2015.12.16
	static public String lget(String key, String defaultval)
	{
		if ( jedisSentinelPool == null )
			return defaultval;

		synchronized ( syncObj )
		{
			Jedis jedis = null;
			String value = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					List<String> list = jedis.lrange(key, 0, -1);
					
					DBConfigure.m_logger.info("list.size="+list.size());
//				    for( int i = 0 ; i < list.size() ; i++ )
//				    {   
//				           (String[])list.get(i)
//				    }
					if( list.size() == 1 )
					{
						 value =  list.get(0) ;
					}
					if ( value == null )
					{
						value = defaultval;
					}

					DBConfigure.m_logger.info ( "List.lget " + key + " = " + value );
					return value;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return defaultval;
		}
	}
	
	//by himgod 2015.12.16
	//List.lpush()  in Redis
	static public boolean lpush(String key,String value)
	{
		if ( jedisSentinelPool == null )
			return false;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					if ( value == null )
						return false;

					DBConfigure.m_logger.info ( "List.lpush " + key + " = " + value );
					jedis.lpush ( key, value);
					
					return true;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return false;
		}
	}
	
	//by himgod 2015.12.18
	//Set.sadd() in Redis
	static public boolean sadd(String key,String value)
	{
		if ( jedisSentinelPool == null )
			return false;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					if ( value == null )
						return false;

					DBConfigure.m_logger.info ( "Set.sadd() " + key + " = " + value );
					jedis.sadd ( key, value);
						
					return true;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return false;
		}
	}
	
	/* by himgod 2015.12.18
	 * Set.srem() in Redis
	 * delete  'value' in redis's set where the key is 'key'
	 * */
	static public boolean srem(String key,String value)
	{
		if ( jedisSentinelPool == null )
			return false;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					if ( value == null )
						return false;

					DBConfigure.m_logger.info ( "Set.srem() " + key + " = " + value );
					jedis.srem ( key, value);
							
					return true;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return false;
		}
	}
	
	/* by himgod 2015.12.18
	 * Set.srem() in Redis
	 * delete  'value' in redis's set where the key is 'key'
	 * */
	static public String smember(String key,String defaultval)
	{
		if ( jedisSentinelPool == null )
			return defaultval;

		synchronized ( syncObj )
		{
			Jedis jedis = null;
			String value = null;
			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					Set<String> tmpSet = jedis.smembers(key);
					if( !tmpSet.isEmpty() )
					{
						Iterator<String> it = tmpSet.iterator();
						value = it.next();
					}
					else
					{
						return defaultval;
					}

					DBConfigure.m_logger.info ( "Set.smember()'s first value " + key + " = " + value );
					
							
					return value;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}

			return defaultval;
		}
	}
	
	/**
	 * hash¶ÁÐ´²Ù×÷
	 * 
	 * @param hash
	 * @param key
	 * @return
	 */
	public static String hash_get(String key, String field, String defaultStr) {
		String result = null;

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					
					result = jedis.hget(key, field);
					DBConfigure.m_logger.info( "hash_get key=" + key + ", field=" + field + ", result = " + result);
					
					if(result == null)
					{
						return defaultStr;
					}
					
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}
		}
		return result;
	}
	
	/**
	 * hash¶ÁÐ´²Ù×÷
	 * 
	 * @param hash
	 * @param key
	 * @return
	 */
	public static boolean hash_set(String key, String field, String value) {

		synchronized ( syncObj )
		{
			Jedis jedis = null;

			try
			{
				jedis = jedisSentinelPool.getResource ();

				if ( jedisSentinelPool != null && jedis != null )
				{
					DBConfigure.m_logger.info( "hash_set key=" + key + ", field=" + field + ", value = " + value);
					
					jedis.hset(key, field, value);
					return true;
				}
			}
			catch ( Exception ex )
			{
				DBConfigure.m_logger.error ( ex.toString (), ex );
				jedisSentinelPool.returnBrokenResource ( jedis );
			}
			finally
			{
				jedisSentinelPool.returnResource ( jedis );
			}
		}
		return false;
	}
}
