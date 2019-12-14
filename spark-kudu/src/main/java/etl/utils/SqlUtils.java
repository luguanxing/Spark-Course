package etl.utils;

public class SqlUtils {

    public static String SQL = "select " +
            "data.ip ," +
            "data.sessionid," +
            "data.advertisersid," +
            "data.adorderid," +
            "data.adcreativeid," +
            "data.adplatformproviderid" +
            ",data.sdkversion" +
            ",data.adplatformkey" +
            ",data.putinmodeltype" +
            ",data.requestmode" +
            ",data.adprice" +
            ",data.adppprice" +
            ",data.requestdate" +
            ",data.appid" +
            ",data.appname" +
            ",data.uuid, data.device, data.client, data.osversion, data.density, data.pw, data.ph" +
            ",ip_info.province as provincename" +
            ",ip_info.city as cityname" +
            ",ip_info.isp as isp" +
            ",data.ispid, data.ispname" +
            ",data.networkmannerid, data.networkmannername, data.iseffective, data.isbilling" +
            ",data.adspacetype, data.adspacetypename, data.devicetype, data.processnode, data.apptype" +
            ",data.district, data.paymode, data.isbid, data.bidprice, data.winprice, data.iswin, data.cur" +
            ",data.rate, data.cnywinprice, data.imei, data.mac, data.idfa, data.openudid,data.androidid" +
            ",data.rtbprovince,data.rtbcity,data.rtbdistrict,data.rtbstreet,data.storeurl,data.realip" +
            ",data.isqualityapp,data.bidfloor,data.aw,data.ah,data.imeimd5,data.macmd5,data.idfamd5" +
            ",data.openudidmd5,data.androididmd5,data.imeisha1,data.macsha1,data.idfasha1,data.openudidsha1" +
            ",data.androididsha1,data.uuidunknow,data.userid,data.iptype,data.initbidprice,data.adpayment" +
            ",data.agentrate,data.lomarkrate,data.adxrate,data.title,data.keywords,data.tagid,data.callbackdate" +
            ",data.channelid,data.mediatype,data.email,data.tel,data.sex,data.age " +
            "from data left join " +
            "ip_info on data.ip_long between ip_info.start and ip_info.end ";

    public static String PROVINCE_CITY_SQL = "select provincename, cityname, count(1) as cnt from ods group by provincename, cityname";

    public static String AREA_SQL_STEP1 = "select provincename,cityname, " +
            "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) origin_request," +
            "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) valid_request," +
            "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) ad_request," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) bid_cnt," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) bid_success_cnt," +
            "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_display_cnt," +
            "sum(case when requestmode=3 and processnode=1 then 1 else 0 end) ad_click_cnt," +
            "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_display_cnt," +
            "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_click_cnt," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*winprice/1000 else 0 end) ad_consumption," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*adpayment/1000 else 0 end) ad_cost " +
            "from ods group by provincename,cityname";

    public static String AREA_SQL_STEP2 = "select provincename,cityname, " +
            "origin_request," +
            "valid_request," +
            "ad_request," +
            "bid_cnt," +
            "bid_success_cnt," +
            "bid_success_cnt/bid_cnt bid_success_rate," +
            "ad_display_cnt," +
            "ad_click_cnt," +
            "ad_click_cnt/ad_display_cnt ad_click_rate," +
            "ad_consumption," +
            "ad_cost from area_tmp " +
            "where bid_cnt!=0 and ad_display_cnt!=0";

    public static String APP_SQL_STEP1 = "select appid,appname, " +
            "sum(case when requestmode=1 and processnode >=1 then 1 else 0 end) origin_request," +
            "sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) valid_request," +
            "sum(case when requestmode=1 and processnode =3 then 1 else 0 end) ad_request," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) bid_cnt," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) bid_success_cnt," +
            "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) ad_display_cnt," +
            "sum(case when requestmode=3 and processnode=1 then 1 else 0 end) ad_click_cnt," +
            "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_display_cnt," +
            "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) medium_click_cnt," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*winprice/1000 else 0 end) ad_consumption," +
            "sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>20000  then 1*adpayment/1000 else 0 end) ad_cost " +
            "from ods group by appid,appname";


    public static String APP_SQL_STEP2 = "select appid,appname, " +
            "origin_request," +
            "valid_request," +
            "ad_request," +
            "bid_cnt," +
            "bid_success_cnt," +
            "bid_success_cnt/bid_cnt bid_success_rate," +
            "ad_display_cnt," +
            "ad_click_cnt," +
            "ad_click_cnt/ad_display_cnt ad_click_rate," +
            "ad_consumption," +
            "ad_cost from app_tmp " +
            "where bid_cnt!=0 and ad_display_cnt!=0";

}
