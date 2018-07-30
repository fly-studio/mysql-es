package com.fly.sync.contract;

import com.fly.sync.setting.River;

import java.util.Timer;
import java.util.TimerTask;

public abstract class AbstractConnector {

    private static final int INTERVAL = 5000;

    private Timer timer = new Timer(true);
    private TimerTask task = new TimerTask() {
        @Override
        public void run() {
            heartbeat(autoReconnect);
        }
    };

    private River river;
    private boolean autoReconnect, connected;
    private ConnectionListener listener;


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

    public boolean connect()
    {
        doConnecting();

        heartbeat(false);
        if (!isConnected())
            return false;

        if (null != listener) listener.onConnected(this);

        timer.cancel();
        timer.schedule(task, INTERVAL, INTERVAL);

        return true;
    }

    public void reconnect()
    {
        doReconnect();
    }

    public synchronized void heartbeat(boolean autoReconnect)
    {
        if (doHeartbeat()) {
            connected = true;
        } else {
            if (null != listener && connected) listener.onDisconnected(this);
            connected = false;

            if (autoReconnect) reconnect();
        }
    }

    public void close()
    {
        timer.cancel();
        doClose();
        if (null != listener && connected) listener.onDisconnected(this);
        connected = false;
    }

    protected abstract void doConnecting();
    protected abstract void doReconnect();
    protected abstract boolean doHeartbeat();
    protected abstract void doClose();


    public boolean isConnected()
    {
        return connected;
    }

    public interface ConnectionListener {
        void onConnected(AbstractConnector connector);
        void onDisconnected(AbstractConnector connector);
    }
}
