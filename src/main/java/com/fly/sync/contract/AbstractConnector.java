package com.fly.sync.contract;

import com.fly.sync.exception.OutOfRetryException;
import com.fly.sync.setting.River;

import java.util.Timer;
import java.util.TimerTask;

public abstract class AbstractConnector {

    private static final int INTERVAL = 5000;

    private Timer timer;
    private TimerTask task = new TimerTask() {
        @Override
        public void run() {
            heartbeat(autoReconnect);
        }
    };

    protected River river;
    protected boolean autoReconnect, connected;
    protected ConnectionListener listener;


    public AbstractConnector(River river, boolean autoReconnect) {
        this.river = river;
        this.autoReconnect = autoReconnect;
        this.connected = false;
    }

    public void setListener(ConnectionListener listener) {
        this.listener = listener;
    }

    public ConnectionListener getListener() {
        return listener;
    }

    public River getRiver() {
        return river;
    }

    public boolean connect() throws Exception
    {
        doConnecting();

        tryHeartbeat();
        if (!isConnected())
            return false;

        if (null != listener) listener.onConnected(this);

        if (null == timer)
        {
            timer = new Timer(true);
            timer.schedule(task, INTERVAL, INTERVAL);
        }

        return true;
    }

    public void reconnect() throws Exception
    {
        doReconnect();
    }

    public void waitForConnected(int count, int sleep)
    {
        int i = 0;
        while (!isConnected()) {
            if (count > 0 && ++i > count)
                throw new OutOfRetryException("Try to reconnect for " + String.valueOf(count) + " times, Still Failed.");
            try {
                reconnect();

            }  catch (Exception e) {

                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e1)
                {
                    return;
                }
            }
        }
    }

    public synchronized void tryHeartbeat() throws Exception
    {
        doHeartbeat();
    }

    public synchronized void heartbeat(boolean autoReconnect) {
        try {
            doHeartbeat();
            connected = true;
        } catch (Exception e)
        {
            connected = false;
            throwError(e);

            if (null != listener && connected) listener.onDisconnected(this);

            if (autoReconnect)
                try {
                    reconnect();
                } catch (Exception e1)
                {
                    throwError(e1);
                }
        }

    }

    public void close() throws Exception
    {
        if (null != timer) {
            timer.cancel();
            timer = null;
        }

        doClose();

        boolean _connected = connected;
        connected = false;

        if (null != listener && _connected) listener.onDisconnected(this);

    }

    protected abstract void doConnecting() throws Exception;
    protected abstract void doReconnect() throws Exception;
    protected abstract void doHeartbeat() throws Exception;
    protected abstract void doClose() throws Exception;


    public boolean isConnected()
    {
        return connected;
    }

    protected void throwError(Exception e)
    {
        if (null != listener) listener.onError(this, e);
    }

    public interface ConnectionListener {
        void onConnected(AbstractConnector connector);
        void onDisconnected(AbstractConnector connector);
        void onError(AbstractConnector connector, Exception e);
    }
}
