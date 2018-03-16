package com.gtexpanse.app.expanse.es;

import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * es client
 */
public class ElasticSearchClient {

    private static final Logger log = LoggerFactory.getLogger("EXPANSE-ES");

    private Client client;
    private String clusterName;
    private String clusterUrls;

    public ElasticSearchClient(String clusterName, String clusterUrls) {
        super();
        this.clusterName = clusterName;
        this.clusterUrls = clusterUrls;
        createTransportClient();
    }

    private void createTransportClient() {
        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportClient tClient = TransportClient.builder().settings(settings).build();
        String[] urlList = clusterUrls.split(",");
        for (String url : urlList) {
            try {
                String[] ips = url.split(":");
                tClient.addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(ips[0]), Integer.parseInt(ips[1])));
            } catch (UnknownHostException e) {
                log.error("unknown host:" + url, e);
            }
        }
        client = tClient;
    }

    public void close() {
        if (client != null)
            client.close();
    }

    /**
     * delete document by id
     *
     * @param index index
     * @param type  type
     * @param id    id
     */
    public void deleteDocument(String index, String type, String id) {
        client.prepareDelete(index, type, id).get();
    }

    /**
     * upsert document -- 如果文档不存在，则会新增该文档
     *
     * @param index index
     * @param type  type
     * @param id    id
     * @param map   source
     */
    public void upsertDocument(String index, String type, String id, Map<String, Object> map) {
        IndexResponse res = client.prepareIndex(index, type, id).setSource(map).get();
        log.info("IndexResponse >> index=" + res.getIndex() + ", type=" + res.getType() + ", id=" + res.getId() + ", version="
                + res.getVersion());
    }

    public ArrayList getMappingFields(String index, String type) {
        try {
            AdminClient aClient = client.admin();
            GetMappingsResponse response = aClient.indices().prepareGetMappings(index).addTypes(type).execute()
                    .actionGet();
            Map<String, Object> map = response.getMappings().valuesIt().next().get(type).sourceAsMap();
            Map fieldMap = (Map) map.get("properties");
            Set set = fieldMap.keySet();
            return Lists.newArrayList(set);
        } catch (Exception e) {
            log.error("get Mapping Fields occur error", e);
        }
        return null;
    }
}
