package com.orangefunction.tomcat.redissessions;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.session.StandardManager;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.catalina.util.SessionIdGenerator;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.util.Pool;

public class RedisSessionManager extends StandardManager
{

    // private static ExecutorService executorService = Executors.newCachedThreadPool();

    enum SessionPersistPolicy {
        DEFAULT,
        SAVE_ON_CHANGE,
        ALWAYS_SAVE_AFTER_REQUEST;

        static SessionPersistPolicy fromName(String name)
        {
            for (SessionPersistPolicy policy : SessionPersistPolicy.values())
            {
                if (policy.name().equalsIgnoreCase(name))
                {
                    return policy;
                }
            }
            throw new IllegalArgumentException("Invalid session persist policy [" + name + "]. Must be one of " + Arrays.asList(SessionPersistPolicy.values()) + ".");
        }
    }

    private final Log log = LogFactory.getLog(RedisSessionManager.class);

    protected SessionPipeMBean sessionPipe = new SessionPipe();

    protected String host = "localhost";

    protected int port = 6379;

    protected int database = 0;

    protected String password = null;

    protected int timeout = Protocol.DEFAULT_TIMEOUT;

    protected String sentinelMaster = null;

    Set<String> sentinelSet = null;

    protected Pool<Jedis> connectionPool;

    protected JedisPoolConfig connectionPoolConfig = new JedisPoolConfig();

    protected Serializer serializer;
    
    
    public Serializer getSerializer()
    {
        return serializer;
    }

    protected static String name = "RedisSessionManager";

    protected String serializationStrategyClass = "com.orangefunction.tomcat.redissessions.JavaSerializer";

    protected EnumSet<SessionPersistPolicy> sessionPersistPoliciesSet = EnumSet.of(SessionPersistPolicy.DEFAULT);

    /**
     * The lifecycle event support for this component.
     */
    protected LifecycleSupport lifecycle = new LifecycleSupport(this);

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public int getDatabase()
    {
        return database;
    }

    public void setDatabase(int database)
    {
        this.database = database;
    }

    public int getTimeout()
    {
        return timeout;
    }

    public void setTimeout(int timeout)
    {
        this.timeout = timeout;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public void setSerializationStrategyClass(String strategy)
    {
        this.serializationStrategyClass = strategy;
    }

    public String getSessionPersistPolicies()
    {
        StringBuilder policies = new StringBuilder();
        for (Iterator<SessionPersistPolicy> iter = this.sessionPersistPoliciesSet.iterator(); iter.hasNext();)
        {
            SessionPersistPolicy policy = iter.next();
            policies.append(policy.name());
            if (iter.hasNext())
            {
                policies.append(",");
            }
        }
        return policies.toString();
    }

    public void setSessionPersistPolicies(String sessionPersistPolicies)
    {
        String[] policyArray = sessionPersistPolicies.split(",");
        EnumSet<SessionPersistPolicy> policySet = EnumSet.of(SessionPersistPolicy.DEFAULT);
        for (String policyName : policyArray)
        {
            SessionPersistPolicy policy = SessionPersistPolicy.fromName(policyName);
            policySet.add(policy);
        }
        this.sessionPersistPoliciesSet = policySet;
    }

    public boolean getSaveOnChange()
    {
        return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.SAVE_ON_CHANGE);
    }

    public boolean getAlwaysSaveAfterRequest()
    {
        return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.ALWAYS_SAVE_AFTER_REQUEST);
    }

    public String getSentinels()
    {
        StringBuilder sentinels = new StringBuilder();
        for (Iterator<String> iter = this.sentinelSet.iterator(); iter.hasNext();)
        {
            sentinels.append(iter.next());
            if (iter.hasNext())
            {
                sentinels.append(",");
            }
        }
        return sentinels.toString();
    }

    public void setSentinels(String sentinels)
    {
        if (null == sentinels)
        {
            sentinels = "";
        }

        String[] sentinelArray = sentinels.split(",");
        this.sentinelSet = new HashSet<String>(Arrays.asList(sentinelArray));
    }

    public Set<String> getSentinelSet()
    {
        return this.sentinelSet;
    }

    public String getSentinelMaster()
    {
        return this.sentinelMaster;
    }

    public void setSentinelMaster(String master)
    {
        this.sentinelMaster = master;
    }

    protected Jedis acquireConnection()
    {
        Jedis jedis = connectionPool.getResource();

        if (getDatabase() != 0)
        {
            jedis.select(getDatabase());
        }

        return jedis;
    }

    protected void returnConnection(Jedis jedis, Boolean error)
    {
        if (error)
        {
            connectionPool.returnBrokenResource(jedis);
        } else
        {
            connectionPool.returnResource(jedis);
        }
    }

    protected void returnConnection(Jedis jedis)
    {
        returnConnection(jedis, false);
    }

    private void registerMean()
    {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        try
        {
            this.sessionPipe.setSessionManager(this);
            this.sessionPipe.setSessionMap(this.sessions);
            mBeanServer.registerMBean(this.sessionPipe, new ObjectName("tomcat.Redis.plugin:name=ISessionPipe"));
        } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Start this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#startInternal()}.
     * 
     * @exception LifecycleException
     *                if this component detects a fatal error
     *                that prevents this component from being used
     */
    @Override
    protected synchronized void startInternal() throws LifecycleException
    {
        /* super.startInternal(); */
        /**
         * @see org.apache.catalina.session.ManagerBase#startInternal() Begin
         */
        while (super.sessionCreationTiming.size() < 100)
        {
            super.sessionCreationTiming.add(null);
        }
        while (super.sessionExpirationTiming.size() < 100)
        {
            super.sessionExpirationTiming.add(null);
        }
        super.sessionIdGenerator = new SessionIdGenerator();
        super.sessionIdGenerator.setJvmRoute(getJvmRoute());
        super.sessionIdGenerator.setSecureRandomAlgorithm(getSecureRandomAlgorithm());
        super.sessionIdGenerator.setSecureRandomClass(getSecureRandomClass());
        super.sessionIdGenerator.setSecureRandomProvider(getSecureRandomProvider());
        super.sessionIdGenerator.setSessionIdLength(getSessionIdLength());
        if (this.log.isDebugEnabled())
        {
            this.log.debug("Force random number initialization starting");
        }
        super.sessionIdGenerator.generateSessionId();
        if (this.log.isDebugEnabled())
        {
            this.log.debug("Force random number initialization completed");
        }
        /**
         * @see org.apache.catalina.session.ManagerBase#startInternal() End
         */
        setState(LifecycleState.STARTING);

        registerMean();
        try
        {
            initializeSerializer();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e)
        {
            log.fatal("Unable to load serializer", e);
            throw new LifecycleException(e);
        }

        log.info("Will expire sessions after " + getMaxInactiveInterval() + " seconds");

        initializeDatabaseConnection();

        setDistributable(true);
    }

    /**
     * Stop this component and implement the requirements
     * of {@link org.apache.catalina.util.LifecycleBase#stopInternal()}.
     * 
     * @exception LifecycleException
     *                if this component detects a fatal error
     *                that prevents this component from being used
     */
    @Override
    protected synchronized void stopInternal() throws LifecycleException
    {
        if (log.isDebugEnabled())
        {
            log.debug("Stopping");
        }

        setState(LifecycleState.STOPPING);

        try
        {
            connectionPool.destroy();
        } catch (Exception e)
        {
            // Do nothing.
        }

        // Require a new random number generator if we are restarted
        /* super.stopInternal(); */
        /**
         * @see org.apache.catalina.session.ManagerBase#stopInternal()
         */
        super.sessionIdGenerator = null;
    }

    

   

   

    public void save(RedisSession session, boolean forceSave) throws IOException
    {
        Jedis jedis = null;
        Boolean error = true;

        try
        {
            jedis = acquireConnection();
            error = saveInternal(jedis, session, forceSave);
        } catch (IOException e)
        {
            throw e;
        } finally
        {
            if (jedis != null)
            {
                returnConnection(jedis, error);
            }
        }
    }

    private boolean saveInternal(Jedis jedis, RedisSession redisSession, boolean forceSave) throws IOException
    {
        Boolean error = true;

        try
        {
            log.trace("Saving session " + redisSession + " into Redis");

            // RedisSession redisSession = (RedisSession)session;

            if (log.isTraceEnabled())
            {
                log.trace("Session Contents [" + redisSession.getId() + "]:");
                Enumeration en = redisSession.getAttributeNames();
                while (en.hasMoreElements())
                {
                    log.trace("  " + en.nextElement());
                }
            }

            byte[] binaryId = redisSession.getId().getBytes();

            byte[] sessionAttributesHash = null;
            if (forceSave || redisSession.isDirty())
            {

                log.trace("Save was determined to be necessary");

                if (null == sessionAttributesHash)
                {
                    sessionAttributesHash = serializer.attributesHashFrom(redisSession);
                }

                SessionSerializationMetadata updatedSerializationMetadata = new SessionSerializationMetadata();
                updatedSerializationMetadata.setSessionAttributesHash(sessionAttributesHash);

                jedis.set(binaryId, serializer.serializeFrom(redisSession, updatedSerializationMetadata));

                redisSession.resetDirtyTracking();
            } else
            {
                log.trace("Save was determined to be unnecessary");
            }

            log.trace("Setting expire timeout on session [" + redisSession.getId() + "] to " + getMaxInactiveInterval());
            jedis.expire(binaryId, getMaxInactiveInterval());

            error = false;

            return error;
        } catch (IOException e)
        {
            log.error(e.getMessage());

            throw e;
        } finally
        {
            return error;
        }
    }

    private void initializeDatabaseConnection() throws LifecycleException
    {
        try
        {
            if (getSentinelMaster() != null)
            {
                Set<String> sentinelSet = getSentinelSet();
                if (sentinelSet != null && sentinelSet.size() > 0)
                {
                    connectionPool = new JedisSentinelPool(getSentinelMaster(), sentinelSet, this.connectionPoolConfig, getTimeout(), getPassword());
                } else
                {
                    throw new LifecycleException("Error configuring Redis Sentinel connection pool: expected both `sentinelMaster` and `sentiels` to be configured");
                }
            } else
            {
                connectionPool = new JedisPool(this.connectionPoolConfig, getHost(), getPort(), getTimeout(), getPassword());
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            throw new LifecycleException("Error connecting to Redis", e);
        }
    }

    private void initializeSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException
    {
        log.info("Attempting to use serializer :" + serializationStrategyClass);
        serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();

        Loader loader = null;

        if (getContainer() != null)
        {
            loader = getContainer().getLoader();
        }

        ClassLoader classLoader = null;

        if (loader != null)
        {
            classLoader = loader.getClassLoader();
        }
        serializer.setClassLoader(classLoader);
    }

    // Connection Pool Config Accessors

    // - from org.apache.commons.pool2.impl.GenericObjectPoolConfig

    public int getConnectionPoolMaxTotal()
    {
        return this.connectionPoolConfig.getMaxTotal();
    }

    public void setConnectionPoolMaxTotal(int connectionPoolMaxTotal)
    {
        this.connectionPoolConfig.setMaxTotal(connectionPoolMaxTotal);
    }

    public int getConnectionPoolMaxIdle()
    {
        return this.connectionPoolConfig.getMaxIdle();
    }

    public void setConnectionPoolMaxIdle(int connectionPoolMaxIdle)
    {
        this.connectionPoolConfig.setMaxIdle(connectionPoolMaxIdle);
    }

    public int getConnectionPoolMinIdle()
    {
        return this.connectionPoolConfig.getMinIdle();
    }

    public void setConnectionPoolMinIdle(int connectionPoolMinIdle)
    {
        this.connectionPoolConfig.setMinIdle(connectionPoolMinIdle);
    }

    // - from org.apache.commons.pool2.impl.BaseObjectPoolConfig

    public boolean getLifo()
    {
        return this.connectionPoolConfig.getLifo();
    }

    public void setLifo(boolean lifo)
    {
        this.connectionPoolConfig.setLifo(lifo);
    }

    public long getMaxWaitMillis()
    {
        return this.connectionPoolConfig.getMaxWaitMillis();
    }

    public void setMaxWaitMillis(long maxWaitMillis)
    {
        this.connectionPoolConfig.setMaxWaitMillis(maxWaitMillis);
    }

    public long getMinEvictableIdleTimeMillis()
    {
        return this.connectionPoolConfig.getMinEvictableIdleTimeMillis();
    }

    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis)
    {
        this.connectionPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
    }

    public long getSoftMinEvictableIdleTimeMillis()
    {
        return this.connectionPoolConfig.getSoftMinEvictableIdleTimeMillis();
    }

    public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis)
    {
        this.connectionPoolConfig.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
    }

    public int getNumTestsPerEvictionRun()
    {
        return this.connectionPoolConfig.getNumTestsPerEvictionRun();
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun)
    {
        this.connectionPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
    }

    public boolean getTestOnCreate()
    {
        return this.connectionPoolConfig.getTestOnCreate();
    }

    public void setTestOnCreate(boolean testOnCreate)
    {
        this.connectionPoolConfig.setTestOnCreate(testOnCreate);
    }

    public boolean getTestOnBorrow()
    {
        return this.connectionPoolConfig.getTestOnBorrow();
    }

    public void setTestOnBorrow(boolean testOnBorrow)
    {
        this.connectionPoolConfig.setTestOnBorrow(testOnBorrow);
    }

    public boolean getTestOnReturn()
    {
        return this.connectionPoolConfig.getTestOnReturn();
    }

    public void setTestOnReturn(boolean testOnReturn)
    {
        this.connectionPoolConfig.setTestOnReturn(testOnReturn);
    }

    public boolean getTestWhileIdle()
    {
        return this.connectionPoolConfig.getTestWhileIdle();
    }

    public void setTestWhileIdle(boolean testWhileIdle)
    {
        this.connectionPoolConfig.setTestWhileIdle(testWhileIdle);
    }

    public long getTimeBetweenEvictionRunsMillis()
    {
        return this.connectionPoolConfig.getTimeBetweenEvictionRunsMillis();
    }

    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis)
    {
        this.connectionPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
    }

    public String getEvictionPolicyClassName()
    {
        return this.connectionPoolConfig.getEvictionPolicyClassName();
    }

    public void setEvictionPolicyClassName(String evictionPolicyClassName)
    {
        this.connectionPoolConfig.setEvictionPolicyClassName(evictionPolicyClassName);
    }

    public boolean getBlockWhenExhausted()
    {
        return this.connectionPoolConfig.getBlockWhenExhausted();
    }

    public void setBlockWhenExhausted(boolean blockWhenExhausted)
    {
        this.connectionPoolConfig.setBlockWhenExhausted(blockWhenExhausted);
    }

    public boolean getJmxEnabled()
    {
        return this.connectionPoolConfig.getJmxEnabled();
    }

    public void setJmxEnabled(boolean jmxEnabled)
    {
        this.connectionPoolConfig.setJmxEnabled(jmxEnabled);
    }

    public String getJmxNameBase()
    {
        return this.connectionPoolConfig.getJmxNameBase();
    }

    public void setJmxNameBase(String jmxNameBase)
    {
        this.connectionPoolConfig.setJmxNameBase(jmxNameBase);
    }

    public String getJmxNamePrefix()
    {
        return this.connectionPoolConfig.getJmxNamePrefix();
    }

    public void setJmxNamePrefix(String jmxNamePrefix)
    {
        this.connectionPoolConfig.setJmxNamePrefix(jmxNamePrefix);
    }
}

class DeserializedSessionContainer
{
    public final RedisSession session;

    public final SessionSerializationMetadata metadata;

    public DeserializedSessionContainer(RedisSession session, SessionSerializationMetadata metadata)
    {
        this.session = session;
        this.metadata = metadata;
    }
}

class SessionPipe implements SessionPipeMBean
{
    private final Log log = LogFactory.getLog(SessionPipe.class);

    private Map<String, Session> sessionMap;

    public void setSessionMap(Map<String, Session> sessionMap)
    {
        this.sessionMap = sessionMap;
    }

    private RedisSessionManager sessionManager;
    
    public void setSessionManager(RedisSessionManager sessionManager)
    {
        this.sessionManager = sessionManager;
    }

    /**
     * Restore all sessions from redis database to tomcat web server
     * 
     * @throws IOException
     */
    public void restoreAllSessions() throws IOException
    {
        Jedis jedis = null;
        try
        {
            log.trace("Attempting to load all session from Redis");
            jedis = this.sessionManager.acquireConnection();
            synchronized (this.sessionMap)
            {
                Set<String> sessionIdSet = jedis.keys("*");
                this.log.info(sessionIdSet);
                if (this.log.isDebugEnabled())
                {
                    this.log.debug("Loading " + sessionIdSet.size() + " persisted sessions");
                }
                for (String id : sessionIdSet)
                {
                    byte[] data = jedis.get(id.getBytes());
                    if (data != null)
                    {
                        DeserializedSessionContainer container = sessionFromSerializedData(id, data);
                        StandardSession session = toSession(container.session);
                        if (this.sessionMap.containsKey(session.getIdInternal()))
                        {
                            continue;// If the tomcat web server contains current session, don't overwrite it.
                        }
                        this.sessionMap.put(session.getIdInternal(), session);
                        session.activate();
                        if (!session.isValid())
                        {
                            session.setValid(true);
                            session.expire();
                        }
                        this.sessionManager.setSessionCounter(this.sessionManager.getSessionCounter() + 1L);
                    }
                }
            }
            if (this.log.isDebugEnabled())
            {
                this.log.debug("Finish: Loading persisted sessions");
            }
        } finally
        {
            if (jedis != null)
            {
                this.sessionManager.returnConnection(jedis, false);
            }
        }
    }

    private StandardSession toSession(final RedisSession redisSession)
    {
        StandardSession session = new StandardSession(this.sessionManager);
        session.setId(redisSession.getId());
        session.setValid(redisSession.isValid());
        session.setAuthType(redisSession.getAuthType());
        session.setCreationTime(redisSession.getCreationTime());
        session.setMaxInactiveInterval(redisSession.getMaxInactiveInterval());
        String noteName = null;
        Iterator<String> nodeNameItr = redisSession.getNoteNames();
        while (nodeNameItr.hasNext())
        {
            noteName = nodeNameItr.next();
            if (noteName != null)
            {
                session.setNote(noteName, redisSession.getNote(noteName));
            }
        }
        session.setNew(redisSession.isNew());
        String attributeName = null;
        Enumeration<String> attributeNameEnum = redisSession.getAttributeNames();
        while (attributeNameEnum.hasMoreElements())
        {
            attributeName = attributeNameEnum.nextElement();
            session.setAttribute(attributeName, redisSession.getAttribute(attributeName));
        }
        return session;
    }
    
    private DeserializedSessionContainer sessionFromSerializedData(String id, byte[] data) throws IOException
    {
        log.trace("Deserializing session " + id + " from Redis");

        if (Arrays.equals("null".getBytes(), data))
        {
            log.error("Encountered serialized session " + id + " with data equal to NULL_SESSION. This is a bug.");
            throw new IOException("Serialized session data was equal to NULL_SESSION");
        }

        RedisSession session = null;
        SessionSerializationMetadata metadata = new SessionSerializationMetadata();

        try
        {
            session = new RedisSession(this.sessionManager);

            this.sessionManager.getSerializer().deserializeInto(data, session, metadata);

            session.setId(id);
            session.setNew(false);
            session.setMaxInactiveInterval(this.sessionManager.getMaxInactiveInterval());
            session.access();
            session.setValid(true);
            session.resetDirtyTracking();

            if (log.isTraceEnabled())
            {
                log.trace("Session Contents [" + id + "]:");
                Enumeration en = session.getAttributeNames();
                while (en.hasMoreElements())
                {
                    log.trace("  " + en.nextElement());
                }
            }
        } catch (ClassNotFoundException ex)
        {
            log.fatal("Unable to deserialize into session", ex);
            throw new IOException("Unable to deserialize into session", ex);
        }

        return new DeserializedSessionContainer(session, metadata);
    }

    /**
     * Backup all sessions from tomcat web server to redis database
     * 
     * @throws IOException
     * 
     */
    public void backupAllSessions() throws IOException
    {
        for (Session session : this.sessionMap.values())
        {
            try
            {
                this.sessionManager.save(fromSession(session), true);
            } catch (IOException ex)
            {
                log.warn("Unable to add to session manager store: " + ex.getMessage());
                throw new IOException("Unable to add to session manager store.", ex);
            }
        }
    }
    
    public int getSessionCount() throws IOException
    {
        Jedis jedis = null;
        Boolean error = true;
        try
        {
            jedis = this.sessionManager.acquireConnection();
            int size = jedis.dbSize().intValue();
            error = false;
            return size;
        } finally
        {
            if (jedis != null)
            {
                this.sessionManager.returnConnection(jedis, error);
            }
        }
    }

    private RedisSession fromSession(Session session)
    {
        final RedisSession redisSession = new RedisSession(this.sessionManager);
        redisSession.setId(session.getId());
        redisSession.setValid(session.isValid());
        redisSession.setAuthType(session.getAuthType());
        redisSession.setCreationTime(session.getCreationTime());
        redisSession.setMaxInactiveInterval(session.getMaxInactiveInterval());
        String noteName = null;
        Iterator<String> nodeNameItr = session.getNoteNames();
        while (nodeNameItr.hasNext())
        {
            noteName = nodeNameItr.next();
            if (noteName != null)
            {
                redisSession.setNote(noteName, session.getNote(noteName));
            }
        }
        if (session instanceof StandardSession)
        {
            redisSession.setNew(((StandardSession) session).isNew());
            String attributeName = null;
            Enumeration<String> attributeNameEnum = ((StandardSession) session).getAttributeNames();
            while (attributeNameEnum.hasMoreElements())
            {
                attributeName = attributeNameEnum.nextElement();
                redisSession.setAttribute(attributeName, ((StandardSession) session).getAttribute(attributeName));
            }
        }
        return redisSession;
    }
    
    public void clearAllSessions()
    {
        Jedis jedis = null;
        Boolean error = true;
        try
        {
            jedis = this.sessionManager.acquireConnection();
            jedis.flushDB();
            error = false;
        } finally
        {
            if (jedis != null)
            {
                this.sessionManager.returnConnection(jedis, error);
            }
        }
    }
}