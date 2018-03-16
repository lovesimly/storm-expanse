package com.gtexpanse.app.expanse.biz.impl;

import com.gtexpanse.app.expanse.bean.ExpanseConfig;
import com.gtexpanse.app.expanse.bean.MessageTuple;
import com.gtexpanse.app.expanse.es.ElasticSearchClient;

import java.util.Map;

/**
 * 保单业务处理类
 * <p>调用esPillar提供的rest服务，根据入参比如policy_id，拿到需要往es里面丢的数据，然后丢入es
 * <p>
 * handler作为bolt的直接调用类，尽量不要去打印MessageTuple相关的日志,因为bolt及acker都已经有打印了
 */
public class UserHandlerImpl extends BaseHandler {

    private ElasticSearchClient esClient;
    private ExpanseConfig conf;

    @Override
    public String insert(MessageTuple messageTuple) throws Exception {
        return update(messageTuple);
    }

    @Override
    public String update(MessageTuple messageTuple) throws Exception {
        Map<String, Object> transMap = messageTuple.getMessage();

        // 调用rest服务获取需要丢入es的数据,这里注意出错要返回错误信息
//        String str = getDataFromRest(transMap);
//        Map<String, Object> result = JSON.parseObject(str);
//        if (!(boolean) result.get("success")) {
//            //调用restful服务出错了
//        }

        esClient.upsertDocument(conf.getEs().getIndex(), conf.getEs().getType(),
                String.valueOf(messageTuple.getMessage().get(conf.getIndexMappingId())), messageTuple.getMessage());
        return null;
    }

    @Override
    public String delete(MessageTuple messageTuple) {
        String str = null;
        try {
            // 既然收到物理删除的消息，那直接给从es删掉吧
            esClient.deleteDocument(conf.getEs().getIndex(), conf.getEs().getType(), String.valueOf(messageTuple.getMessage().get(conf.getIndexMappingId())));
        } catch (Exception e) {
            str = e.getCause().toString();
        }
        return str;
    }

    /**
     * 调用rest服务拿到要往es里丢的数据
     *
     * @param map 调用rest服务需要的参数
     * @return source json
     */
    private String getDataFromRest(Map<String, Object> map) {
        return restTemplate.getForObject(conf.getRestServiceUrl() + "/" + map.get("id").toString(), String.class);
    }

    @Override
    public void init(ExpanseConfig config) {
        this.conf = config;
        this.esClient = new ElasticSearchClient(conf.getEs().getClusterName(), conf.getEs().getClusterUrls());
    }

    @Override
    public void cleanup() {
        if (esClient != null) {
            esClient.close();
        }
    }
}
