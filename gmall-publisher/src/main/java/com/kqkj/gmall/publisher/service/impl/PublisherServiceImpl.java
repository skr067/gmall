package com.kqkj.gmall.publisher.service.impl;

import com.kqkj.gmall.common.constant.GmallConstant;
import com.kqkj.gmall.publisher.service.PublishService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublishService{

    @Autowired
    JestClient jestClient;
    //
    @Override
    public Integer getDauTotal(String date) {
        //方式一
        /*String query = "{\n" +
                "  \"query\" : {\n" +
                "    \"bool\" : {\n" +
                "      \"filter\" : {\n" +
                "          \"term\" : {\n" +
                "            \"logDate\" : \"2020-05-02\"\n" +
                "          }\n" +
                "      }\n" +
                "    }\n" +
                "  }";*/
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        Integer total = 0;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map getDauHourMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        //聚合
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.query(boolQueryBuilder).aggregation(aggsBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        Map dauHourMap = new HashMap();
        try{
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets){
                String key = bucket.getKey();
                Long count = bucket.getCount();
                dauHourMap.put(key,count);
            }
        } catch (IOException e){
            e.printStackTrace();
        }
        return dauHourMap;
    }

    @Override
    public Double getOrderAmout(String date) {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"createDate\": \"2019-02-13\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "  \"aggs\": {\n" +
                "         \"sum_totalAmount\": {\n" +
                "           \"sum\": {\n" +
                "             \"field\": \"totalAmount\"\n" +
                "           }\n" +
                "         }\n" +
                "}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        SumBuilder aggsBuilder = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        searchSourceBuilder.query(boolQueryBuilder).aggregation(aggsBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType("_doc").build();
        Double sum_totalAmount = 0D;
        try {
            SearchResult searchResult = jestClient.execute(search);
            sum_totalAmount = searchResult.getAggregations().getSumAggregation("sum_totalAmount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sum_totalAmount;
    }

    @Override
    public Map getOrderAmoutHourMap(String date) {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"createDate\": \"2019-02-13\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }, \n" +
                "  \n" +
                "   \"aggs\": {\n" +
                "     \"tm\": {\n" +
                "       \"terms\": {\n" +
                "         \"field\": \"createHour\"\n" +
                "       }\n" +
                "       , \"aggs\": {\n" +
                "         \"sum_totalAmount\": {\n" +
                "           \"sum\": {\n" +
                "             \"field\": \"totalAmount\"\n" +
                "           }\n" +
                "         }\n" +
                "       }\n" +
                "     }\n" +
                "   }\n" +
                "}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        //聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        //自聚合
        termsBuilder.subAggregation(sumBuilder);
        searchSourceBuilder.query(boolQueryBuilder).aggregation(termsBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType("_doc").build();
        Map<String,Double> hourMap = new HashMap<>();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets){
                Double hourAmount = bucket.getSumAggregation("sum_totalAmount").getSum();
                String hourkey = bucket.getKey();
                hourMap.put(hourkey, hourAmount);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hourMap;
    }

    @Override
    public Map getSaleDetailMap(String date, String keyword, int pageNo, int pageSize, String aggsFieldName, int aggsSize) {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"dt\": \"2019-02-14\"\n" +
                "        }\n" +
                "      }, \n" +
                "      \"must\": [\n" +
                "        {\"match\":{\n" +
                "          \"sku_name\": {\n" +
                "            \"query\": \"小米手机\",\n" +
                "            \"operator\": \"and\"\n" +
                "          }\n" +
                "         } \n" +
                "          \n" +
                "        }\n" +
                "     ] \n" +
                "    }\n" +
                "  }\n" +
                "  , \"aggs\":  {\n" +
                "    \"groupby_age\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_age\" \n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  ,\n" +
                "  \"size\": 2\n" +
                "  , \"from\": 0\n" +
                "}";
        Integer total = 0;
        List<Map> aggsList = new ArrayList<>();
        Map aggsMap = new HashMap();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //过滤
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        //全文匹配
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_" + aggsFieldName).field(aggsFieldName).size(aggsSize);
        searchSourceBuilder.aggregation(termsBuilder);
        //分页
        searchSourceBuilder.from(pageNo-1);
        searchSourceBuilder.size(pageSize);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType("_doc").build();
        try {

            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit hit : hits){
                aggsList.add((Map) hit.source);
            }
            //去聚合结果
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggsFieldName).getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                aggsMap.put(bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map saleMap = new HashMap();
        saleMap.put("total",total);
        saleMap.put("detail",aggsList);
        saleMap.put("aggsMap",aggsMap);
        return saleMap;
    }
}
