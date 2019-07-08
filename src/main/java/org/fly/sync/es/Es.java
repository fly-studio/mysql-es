package org.fly.sync.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.fly.core.io.IoUtils;
import org.fly.core.text.json.Jsonable;
import org.fly.sync.contract.AbstractConnector;
import org.fly.sync.exception.OutOfRetryException;
import org.fly.sync.mysql.type.MySQLJson;
import org.fly.sync.mysql.type.MySQLJsonSerializer;
import org.fly.sync.setting.River;
import org.fly.sync.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

public class Es {

    public Connector connector;
    private River river;
    public final static Logger logger = LoggerFactory.getLogger(Es.class);

    public Es(River river, boolean autoReconnect) {
        this.river = river;
        this.connector = new Connector(river, autoReconnect);
    }

    public boolean connect() throws Exception
    {
        return connector.connect();
    }

    public void close() throws Exception
    {
        connector.close();
    }

    public void waitForConnected(int count, int sleep)
    {
        connector.waitForConnected(count, sleep);
    }


    public RestHighLevelClient getClient() throws OutOfRetryException
    {
        if(!connector.isConnected())
            waitForConnected(10, 5000);

        return connector.getClient();
    }

    public RestClient getRestClient() throws OutOfRetryException
    {
        if (!connector.isConnected())
            waitForConnected(10, 5000);

        return this.connector.getRestClient();
    }

    public void createIndex(River.Table table) throws OutOfRetryException, IOException
    {
        createIndex(table, false);
    }

    public void createIndex(River.Table table, boolean force) throws OutOfRetryException, IOException
    {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(table.index);
        RestHighLevelClient client = getClient();
        boolean existed = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (existed && force)
        {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(table.index);
            client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } else if (existed)
        {
            logger.error("Elastic Index [{}] Exists, Skip created.", table.index);
            return;
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(table.index);
        //With Mapping
        if (table.template != null && !table.template.isEmpty())
        {
           String json = IoUtils.readJson(Setting.getEtcPath(new File(table.template)));
           createIndexRequest.source(json, XContentType.JSON);
        }

        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

    }

    public void createIndices(River.Database database) throws OutOfRetryException, IOException
    {
        createIndices(database, false);
    }

    public void createIndices(River.Database database, boolean force) throws OutOfRetryException, IOException
    {
        for (Map.Entry<String, River.Table> entry: database.tables.entrySet()
                ) {
            River.Table table = entry.getValue();
            if (!table.sync.created || table.isDynamicIndexName()) continue;

            createIndex(table, force);
        }
    }

    public ObjectMapper getJsonMapper() {
        ObjectMapper mapper = Jsonable.Builder.makeAdapter();

        // add MySQLJson Type
        SimpleModule module = new SimpleModule();
        module.addSerializer(MySQLJson.class, new MySQLJsonSerializer());
        mapper.registerModule(module);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        dateFormat.setTimeZone(TimeZone.getTimeZone(river.es.timeZone));
        mapper.setDateFormat(dateFormat);

        return mapper;
    }

    public class Connector extends AbstractConnector {

        private RestClient restClient;
        private RestHighLevelClient client;

        public Connector(River river, boolean autoReconnect) {
            super(river, autoReconnect);
        }

        public RestHighLevelClient getClient()
        {
            return client;
        }

        public RestClient getRestClient() {
            return restClient;
        }

        protected void doConnecting()
        {
            if (null != client)
                return;

            RestClientBuilder builder = RestClient.builder(new HttpHost(river.es.host, river.es.port));

            if (river.es.user != null && river.es.user.length() != 0)
            {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY,
                        new UsernamePasswordCredentials(river.es.user, river.es.password != null ? river.es.password : ""));

                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }

            client = new RestHighLevelClient(builder);
            restClient = client.getLowLevelClient();
        }

        protected void doReconnect() throws Exception
        {
            client = null;
            connect();
        }

        protected void doClose() throws Exception
        {
            if (null != client) {
                client.close();
                restClient.close();
            }

            client = null;
        }

        public void doHeartbeat() throws Exception
        {
            if (!client.ping(RequestOptions.DEFAULT))
                throw new IOException("Ping ElasticSearch Failed.");
        }
    }


}
