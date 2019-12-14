package etl.utils;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;

import java.util.ArrayList;
import java.util.List;

public class SchemaUtils {

    public static Schema getOdsSchema() {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ip", Type.STRING).nullable(false).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("sessionid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("advertisersid",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adorderid", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adcreativeid", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adplatformproviderid", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("sdkversion", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adplatformkey", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("putinmodeltype", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("requestmode", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adprice", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adppprice", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("requestdate", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("appid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("appname", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("uuid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("device", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("client", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("osversion", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("density", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("pw", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ph", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ispid", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("isp", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("iseffective", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("isbilling", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adspacetype", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adspacetypename", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("devicetype", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("processnode", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("apptype", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("district", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("paymode", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("isbid", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bidprice", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("winprice", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("iswin", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("cur", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("rate", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("cnywinprice", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("imei", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("mac", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("idfa", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("openudid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("androidid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("rtbprovince", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("rtbcity", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("rtbdistrict", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("rtbstreet", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("storeurl", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("realip", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("isqualityapp", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bidfloor", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("aw", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ah", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("imeimd5", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("macmd5", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("idfamd5", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("openudidmd5", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("androididmd5", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("imeisha1", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("macsha1", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("idfasha1", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("openudidsha1", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("androididsha1", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("uuidunknow", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("userid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("iptype", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("initbidprice", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adpayment", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("agentrate", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("lomarkrate", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("adxrate", Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("title", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("keywords", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("tagid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("callbackdate", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("mediatype", Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("email", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("tel", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("sex", Type.STRING).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.STRING).nullable(false).build());
        Schema schema = new Schema(columns);
        return schema;
    }

    public static Schema getProvinceCitySchema() {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("provincename",Type.STRING).nullable(false).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("cityname",Type.STRING).nullable(false).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("cnt",Type.INT64).nullable(false).key(true).build());
        Schema schema = new Schema(columns);
        return schema;
    }

    public static Schema getAreaSchema() {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("provincename",Type.STRING).nullable(false).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("cityname",Type.STRING).nullable(false).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("origin_request",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("valid_request",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_request",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bid_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bid_success_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bid_success_rate",Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_display_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_click_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_click_rate",Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_consumption",Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_cost",Type.DOUBLE).nullable(false).build());
        Schema schema = new Schema(columns);
        return schema;
    }

    public static Schema getAppSchema() {
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("appid",Type.STRING).nullable(false).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("appname",Type.STRING).nullable(false).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("origin_request",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("valid_request",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_request",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bid_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bid_success_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("bid_success_rate",Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_display_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_click_cnt",Type.INT64).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_click_rate",Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_consumption",Type.DOUBLE).nullable(false).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("ad_cost",Type.DOUBLE).nullable(false).build());
        Schema schema = new Schema(columns);
        return schema;
    }

}
