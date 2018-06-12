package com.adfunds.core.utils.settlement;

import com.adfunds.core.constants.AdTypeConstants;
import com.adfunds.core.constants.SettlementConstants;
import com.adfunds.core.utils.CaculateUtils;
import com.adfunds.core.utils.DateTimeUtils;
import com.adfunds.core.utils.FileUtil;
import com.adfunds.core.utils.sign.sha.SHA256Util;
import com.adfunds.web.pojo.Ad;
import com.google.gson.Gson;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Description: 结算中心工具类
 * @author: zhangds
 * @since: 2018年04月08日 10时46分57秒
 */
@Component
public class SettlementUtils {
    @Value("${settlement.startdate}")
    private String startDate;
    @Value("#")
    private String accuracy;
    @Value("${settlement.data.basepath}")
    private String basePath;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private DateTimeUtils dateTimeUtils;
    @Autowired
    private SHA256Util sha256Util;
    @Autowired
    private CaculateUtils caculateUtils;
    /**
     * 获得当天发币数量
     * @return
     */
    public double getCoin() {

        double coins = 0.00;
        try {
            Integer year = dateTimeUtils.getYear(new Date());
            if (year <= 2023){
                Map yearInfo = getYearInfo(year);
                if (dateTimeUtils.getFormatDate("yyyy-MM-dd").equals(yearInfo.get("date").toString())){
                    coins = (double)yearInfo.get("begin");
                }else {
                    //计算当年全部天数
                    int all = dateTimeUtils.daysBetween(yearInfo.get("date").toString(),dateTimeUtils.getFormatDate(dateTimeUtils.getYearLast(year),"yyyy-MM-dd"))+1;
                    //计算公差
                    double d = caculateUtils.getD((double) yearInfo.get("begin"), all, (double) yearInfo.get("alladf"));
                    //计算这是当年的第几天
                    int daysBetween = dateTimeUtils.daysBetween(yearInfo.get("date").toString(), dateTimeUtils.getFormatDate(new Date(), "yyyy-MM-dd"))+1;
                    double an = caculateUtils.getAn((double)yearInfo.get("begin"), daysBetween, d);
                    coins = an;
                }
            }else {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = sdf.parse(startDate);
                int j = dateTimeUtils.daysBetween( date,new Date());
                double firstDay = cal(j, 0.03287671232876712328767123287671);
                double secondDay = cal(j + 1, 0.03287671232876712328767123287671);
                double sendcoin = (secondDay - firstDay) * 10000;
                coins = sendcoin;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return coins;
    }

    /**
     * 获得指定日期发币量发币数量
     * @return
     */
    public double getCoin(Date now) {

        double coins = 0.00;
        try {
            Integer year = dateTimeUtils.getYear(now);
            if (year <= 2023){
                Map yearInfo = getYearInfo(year);
                if (dateTimeUtils.getFormatDate("yyyy-MM-dd").equals(yearInfo.get("date").toString())){
                    coins = (double)yearInfo.get("begin");
                }else {
                    //计算当年全部天数
                    int all = dateTimeUtils.daysBetween(yearInfo.get("date").toString(),dateTimeUtils.getFormatDate(dateTimeUtils.getYearLast(year),"yyyy-MM-dd"))+1;
                    //计算公差
                    double d = caculateUtils.getD((double) yearInfo.get("begin"), all, (double) yearInfo.get("alladf"));
                    //计算这是当年的第几天
                    int daysBetween = dateTimeUtils.daysBetween(yearInfo.get("date").toString(), dateTimeUtils.getFormatDate(now, "yyyy-MM-dd"))+1;
                    double an = caculateUtils.getAn((double)yearInfo.get("begin"), daysBetween, d);
                    coins = an;
                }
            }else {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = sdf.parse(startDate);
                int j = dateTimeUtils.daysBetween( date,now);
                double firstDay = cal(j, 0.03287671232876712328767123287671);
                double secondDay = cal(j + 1, 0.03287671232876712328767123287671);
                double sendcoin = (secondDay - firstDay) * 10000;
                coins = sendcoin;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return coins;
    }


    public double cal(int days, double step_length) {
        double t = 0.0;
        for (int i = 1; i < days; i++) {
            t += step_length;
        }
        return 9200 * 1035.16 * Math.exp(0.0212 * t) / (9200 + 1035.16*(Math.exp(0.0212 * t)-1));
    }

    public Map getYearInfo(Integer year){
        HashMap map = new HashMap<>();
        if (year == 2018){
            map.put("date",SettlementConstants.START_DATE_2018);
            map.put("begin",SettlementConstants.BEGIN_2018);
            map.put("alladf",SettlementConstants.ALLADF_2018);
        }else if (year == 2019){
            map.put("date",SettlementConstants.START_DATE_2019);
            map.put("begin",SettlementConstants.BEGIN_2019);
            map.put("alladf",SettlementConstants.ALLADF_2019);
        }else if (year == 2020){
            map.put("date",SettlementConstants.START_DATE_2020);
            map.put("begin",SettlementConstants.BEGIN_2020);
            map.put("alladf",SettlementConstants.ALLADF_2020);
        }else if (year == 2021){
            map.put("date",SettlementConstants.START_DATE_2021);
            map.put("begin",SettlementConstants.BEGIN_2021);
            map.put("alladf",SettlementConstants.ALLADF_2021);
        }else if (year == 2022){
            map.put("date",SettlementConstants.START_DATE_2022);
            map.put("begin",SettlementConstants.BEGIN_2022);
            map.put("alladf",SettlementConstants.ALLADF_2022);
        }else if (year == 2023){
            map.put("date",SettlementConstants.START_DATE_2023);
            map.put("begin",SettlementConstants.BEGIN_2023);
            map.put("alladf",SettlementConstants.ALLADF_2023);
        }
        return map;
    }

    /**
     * 获取流量主贡献数据hash值
     * @param sendDate 结算日期
     * @param advertiser 流量主id
     * @return hash值
     * @throws IOException
     */
    public String getHash(String sendDate,String advertiser) throws IOException {
        Client client = elasticsearchTemplate.getClient();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("sendDate.keyword",sendDate))
                .must(QueryBuilders.termQuery("advertiser.keyword",advertiser));

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("originaladcoin").setTypes("originalad").setScroll(TimeValue.timeValueMinutes(8));
        searchRequestBuilder.setQuery(queryBuilder);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        StringBuffer buffer = new StringBuffer();
        List<Ad> ads = new ArrayList<>();
        while(true){
            for (SearchHit hit : searchResponse.getHits()) {
                Ad ad = new Gson().fromJson(hit.getSourceAsString(), Ad.class);
                ads.add(ad);
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(TimeValue.timeValueMinutes(8))
                    .execute().actionGet();
            if (searchResponse.getHits().getHits().length == 0) {
                break;
            }
        }
        String hash = sha256Util.getSHA256StrJava(ads.toString());
        String realPath = basePath+"/"+sendDate;
        if (!FileUtil.dirExists(realPath)){
            FileUtil.createDir(realPath);
        }

        FileOutputStream out = new FileOutputStream(realPath+"/"+hash);
        out.write(ads.toString().getBytes());
        out.close();
        System.out.println("文件写入成功"+realPath+"/"+hash);
        return hash;

    }

    /**
     * 读取完备数据
     * @param sendDate
     * @param flowMain
     * @param adType
     * @return
     */
    public double getCompleteness(String sendDate,String flowMain,String adType){
        double wanb = 0.0;
        Client client = elasticsearchTemplate.getClient();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("sendDate.keyword",sendDate))
                .must(QueryBuilders.termQuery("flowMain.keyword",flowMain))
                .must(QueryBuilders.termQuery("adType.keyword",adType));

//        SortBuilder sortBuilder = ScriptSortBuilder.fromXContent()
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("adcoin").setTypes("ad");
        searchRequestBuilder.setQuery(queryBuilder);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        if (hits.getTotalHits() != 0){
            for(int i = 0;i< (hits.totalHits>7?7:hits.totalHits);i++){
                SearchHit hit = hits.getAt(i);
                Ad ad = new Gson().fromJson(hit.getSourceAsString(), Ad.class);

                double temp = 0.0;

                if (ad.getSource() != null && !"".equals(ad.getSource()) && !"null".equals(ad.getSource())){
                    temp += 1.0;
                }
                if ((ad.getSourceCountry() != null && !"".equals(ad.getSourceCountry()) && !"null".equals(ad.getSourceCountry()))&&
                        (ad.getSourceProvince() != null && !"".equals(ad.getSourceProvince()) && !"null".equals(ad.getSourceProvince()))&&
                        (ad.getSourceCity() != null && !"".equals(ad.getSourceCity()) && !"null".equals(ad.getSourceCity()))){
                    temp += 7.0;
                }
                if (ad.getSex() != null && !"".equals(ad.getSex()) && !"null".equals(ad.getSex())){
                    temp += 5.0;
                }
                if (ad.getAge() != null && !"".equals(ad.getAge()) && !"null".equals(ad.getAge())){
                    temp += 7.0;
                }
                if (ad.getSourceIndustry() != null && !"".equals(ad.getSourceIndustry()) && !"null".equals(ad.getSourceIndustry())){
                    temp += 7.0;
                }
                if(ad.getSourceOccupation() != null && !"".equals(ad.getSourceOccupation()) && !"null".equals(ad.getSourceOccupation())){
                    temp += 8.0;
                }
                if(ad.getSourceEducation() != null && !"".equals(ad.getSourceEducation()) && !"null".equals(ad.getSourceEducation())){

                    temp += 5.0;
                }
                if (ad.getSourceInterests() != null && !"".equals(ad.getSourceInterests()) && !"null".equals(ad.getSourceInterests())){
                    temp += 7.0;
                }
                if (ad.getSourceSearchWords() != null && !"".equals(ad.getSourceSearchWords()) && !"null".equals(ad.getSourceSearchWords())){
                    temp += 7.0;
                }
                if (ad.getInstallApplication() != null && !"".equals(ad.getInstallApplication()) && !"null".equals(ad.getInstallApplication())){
                    temp += 7.0;
                }
                if (ad.getApplicationRank() != null && !"".equals(ad.getApplicationRank()) && !"null".equals(ad.getApplicationRank())){
                    temp += 7.0;
                }
                if (ad.getFirstActivationTime() != null && !"".equals(ad.getFirstActivationTime()) && !"null".equals(ad.getFirstActivationTime())){
                    temp += 2.0;
                }
                if (ad.getVisiterMostWeb() != null && !"".equals(ad.getVisiterMostWeb()) && !"null".equals(ad.getVisiterMostWeb())){
                    temp += 7.0;
                }

                if (temp > wanb){
                    wanb = temp;
                }

            }

            if ((wanb/60.0) < 0.1){
                wanb = 0.1;
            }else{
                wanb = wanb/60.0;
            }

        }
        return wanb;
    }

    /**
     * 读取广告样式 系数
     * @param adType
     * @return
     */
    public double getStylefactor(String adType) {
        double b = 0.0;
        if (AdTypeConstants.AD_TYPE_001.equals(adType)) {
            b = 0.30;
        }
        if (AdTypeConstants.AD_TYPE_002.equals(adType)) {
            b =  0.28;
        }
        if (AdTypeConstants.AD_TYPE_003.equals(adType)) {
            b =  0.32;
        }
        if (AdTypeConstants.AD_TYPE_004.equals(adType)) {
            b =  0.30;
        }
        if (AdTypeConstants.AD_TYPE_005.equals(adType)) {
            b =  0.35;
        }
        if (AdTypeConstants.AD_TYPE_006.equals(adType)) {
            b =  0.33;
        }
        if (AdTypeConstants.AD_TYPE_007.equals(adType)) {
            b =  0.35;
        }
        if (AdTypeConstants.AD_TYPE_008.equals(adType)) {
            b =  0.33;
        }
        return b;
    }


    /**
     * 获取接单灵活度
     * @return
     */
    public double getFlexibility(String flowMain,String sendDate){
        double flexibility = 0.1;
        Client client = elasticsearchTemplate.getClient();
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("sendDate.keyword",sendDate))
                .must(QueryBuilders.termQuery("flowMain.keyword",flowMain));

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("adcoin").setTypes("ad");
        searchRequestBuilder.setQuery(queryBuilder);
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        if (hits.getTotalHits() != 0) {
            SearchHit hit = hits.getAt(0);
            Ad ad = new Gson().fromJson(hit.getSourceAsString(), Ad.class);
            double v = 1.0 - Double.parseDouble(((null == ad.getIndustryLimit()|| "".equals(ad.getIndustryLimit()) || "null".equals(ad.getIndustryLimit())))?"0":ad.getIndustryLimit()) * 0.05;
            if (v > 0.1){
                flexibility = v;
            }
        }
        return flexibility;
    }

}
