package com.orangefunction.tomcat.redissessions;

import java.io.IOException;
import java.util.Map;

import org.apache.catalina.Session;

public interface SessionPipeMBean
{
    public void setSessionManager(RedisSessionManager sessionManager);
    
    public void setSessionMap(Map<String,Session> sessionMap);
    
    public int getSessionCount() throws IOException;
    
    public void restoreAllSessions() throws IOException;
    
    public void backupAllSessions() throws IOException;
    
    public void clearAllSessions();

    
}
