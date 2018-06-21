package com.adfunds.web.scheduled;

import com.adfunds.core.utils.DateTimeUtils;
import com.adfunds.core.utils.settlement.SettlementUtils;
import com.adfunds.web.pojo.dto.Abnormal;
import com.adfunds.web.pojo.dto.Settlement;
import com.adfunds.web.pojo.dto.Ad;
import com.adfunds.web.service.elastic.IAdService;
import com.adfunds.web.service.fabric.IStateService;
import com.adfunds.web.service.mysql.IAbnormalService;
import com.adfunds.web.service.mysql.ISettlementService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @Description: 结算中心
 * @author: zhangds
 * @since: 2018年04月02日 16时25分02秒
 */
@Configuration
@EnableScheduling
public class SettlementCenter {
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    private Log logger = LogFactory.getLog(this.getClass());
    @Autowired
    private IStateService stateService;

    @Autowired
    private SettlementUtils settlementUtils;
    @Autowired
    private DateTimeUtils dateTimeUtils;
    @Autowired
    private IAdService adService;
    @Autowired
    private ISettlementService settlementService;
    @Autowired
    private IAbnormalService abnormalService;

    @Value("${settlement.noderatio}")
    private double noderatio;


    /**
     * 结算中心定时任务每日凌晨0点开始执行
     * 读取前一日流量主贡献书籍计算流量主贡献按照比例分配ADF
     */
    @Async
    @Scheduled(cron = "${settlement.executionfrequency}")
    public void settlement() {
        try {
            Client client = elasticsearchTemplate.getClient();
            DecimalFormat df = new DecimalFormat("#0.00000000");
            Date yesterday = dateTimeUtils.getDate(-1);
            String sendDate = dateTimeUtils.getFormatDate(yesterday, "yyyyMMdd");
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch("adcoin").setTypes("ad");
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("sendDate.keyword", sendDate));
            TermsAggregationBuilder flowMainAgg = AggregationBuilders.terms("flowMainAgg").field("flowMain.keyword");
            TermsAggregationBuilder adTypeAgg = AggregationBuilders.terms("adTypeAgg").field("adType.keyword");
            flowMainAgg.subAggregation(adTypeAgg);

            SearchResponse searchResponse = searchRequestBuilder
                    .setQuery(queryBuilder)
                    .addAggregation(flowMainAgg)
                    .execute().actionGet();
            //取到当天的发币数量
            double nowcoin = settlementUtils.getCoin();
            //流量主贡献记录
            List<Map> ls = new ArrayList<>();
            //总贡献记录
            double alls = 0.0;
            Terms flowMainTerms = searchResponse.getAggregations().get("flowMainAgg");
            Iterator<? extends Terms.Bucket> flowMainBucketIt = flowMainTerms.getBuckets().iterator();
            while (flowMainBucketIt.hasNext()) {
                Terms.Bucket flowMainBucket = flowMainBucketIt.next();

                if (null != flowMainBucket.getKey() && !"".equals(flowMainBucket.getKey().toString())) {
                    logger.info("流量主 " + flowMainBucket.getKey() + " 有" + flowMainBucket.getDocCount() + "个贡献。");
                    Ad ad = adService.getAddressByFlowMainAndSendDate(flowMainBucket.getKey().toString(), sendDate);

                    StringTerms adTypeTerms = (StringTerms) flowMainBucket.getAggregations().asMap().get("adTypeAgg");
                    Iterator<StringTerms.Bucket> adTypeBucketIt = adTypeTerms.getBuckets().iterator();
                    double zh = 0.0;

                    while (adTypeBucketIt.hasNext()) {
                        Terms.Bucket adTyperBucket = adTypeBucketIt.next();
                        logger.info("样式 " + adTyperBucket.getKey() + " 有" + adTyperBucket.getDocCount() + "个贡献。");

                        double count = (double) adTyperBucket.getDocCount();
                        //计算样式系数
                        double styleFactor = settlementUtils.getStylefactor(adTyperBucket.getKey().toString());
                        double completeness = settlementUtils.getCompleteness(sendDate, flowMainBucket.getKeyAsString(), adTyperBucket.getKeyAsString());

                        System.out.println("样式系数：" + styleFactor);
                        System.out.println("完备程度：" + completeness);

                        double aa = count * styleFactor * completeness * 0.8;
                        System.out.println(adTyperBucket.getKey().toString() + "贡献值" + aa);
                        zh += aa;
                    }

                    double flexibility = settlementUtils.getFlexibility(flowMainBucket.getKey().toString(), sendDate);
                    System.out.println("灵活程度：" + flexibility);
                    double gx = zh * flexibility * 1;
                    Map map = new HashMap();
                    map.put("flowMain", ad.getFlowMain());
                    map.put("nodeId", ad.getNodeId());
                    map.put("gx", gx);
                    map.put("flowMainAddress", ad.getFlowMainAddress());
                    map.put("allianceAddress", ad.getAllianceAddress());
                    ls.add(map);
                    alls += gx;
                }


                logger.info("今天发币量  : " + df.format(nowcoin));
                for (Map map : ls) {
                    String flowMain = map.get("flowMain").toString();
                    String nodeId = map.get("nodeId").toString();
                    double gx = (double) map.get("gx");
                    Object flowMainAddress = map.get("flowMainAddress");
                    Object allianceAddress = map.get("allianceAddress");
                    String hash = settlementUtils.getHash(sendDate, flowMain);
                    System.out.println(hash);
                    double v = gx / alls;
                    logger.info("流量主 " + flowMain + "贡献占比： " + df.format(v));

                    if (null == flowMainAddress || "".equals(flowMainAddress.toString())) {

                        double flowMainADF = nowcoin * v;
                        double allianceADF = flowMainADF * noderatio;

                        if (null != allianceAddress) {
                            //异常结算到节点地址
                            stateService.abnormal(allianceAddress.toString(), df.format(flowMainADF));
                            //节点正常提成
                            stateService.commission(allianceAddress.toString(), df.format(allianceADF));
                            logger.info("流量主获得： " + df.format(flowMainADF - allianceADF) + " 个ADF");
                            logger.info("存入钱包地址：" + flowMainAddress);

                            logger.info("节点获得：" + df.format(allianceADF) + "个ADF");
                            logger.info("存入钱包地址：" + allianceAddress);

                            Abnormal abnormal = new Abnormal();
                            abnormal.setAmount(df.format(flowMainADF));
                            abnormal.setAllianceAddress(allianceAddress.toString());
                            abnormal.setFlowMain(flowMain);
                            abnormal.setNodeId(nodeId);
                            abnormal.setSettlementDate(sendDate);
                            abnormalService.insert(abnormal);
                        }
                    } else {
                        double flowMainADF = nowcoin * v;
                        double allianceADF = flowMainADF * noderatio;
                        stateService.settlement(flowMainAddress.toString(), df.format(flowMainADF), hash);

                        stateService.commission(allianceAddress.toString(), df.format(allianceADF));

                        Settlement settlement = new Settlement();
                        settlement.setFlowMain(flowMain);
                        settlement.setNodeId(nodeId);
                        settlement.setFlowMainAddress(flowMainAddress.toString());
                        settlement.setAllianceAddress(allianceAddress.toString());
                        settlement.setSettlementDate(sendDate);
                        settlement.setFlowMainAdf(df.format(flowMainADF));
                        settlement.setNodeAdf(df.format(allianceADF));

                        settlementService.insert(settlement);
                        logger.info("流量主获得： " + df.format(flowMainADF - allianceADF) + " 个ADF");
                        logger.info("存入钱包地址：" + flowMainAddress);

                        logger.info("节点获得：" + df.format(allianceADF) + "个ADF");
                        logger.info("存入钱包地址：" + allianceAddress);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
