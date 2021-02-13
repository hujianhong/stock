package com.datiehan.stock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

/**
 * @author jianhonghu
 * @date 2021/2/12
 */
public class JisiluSpider {

    private static final OkHttpClient client = new OkHttpClient();

    public static final String URL = "https://www.jisilu.cn/data/new_stock/hkipo/?___jsl=LST___t=" + System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        Request request = new Request.Builder()
                .url(URL)
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) {
            throw new IOException("Unexpected code " + response);
        }

        String responseBody = response.body().string();
        System.out.println(responseBody);
        JSONObject object = JSON.parseObject(responseBody);
        JSONArray jsonArray = object.getJSONArray("rows");
        List<StockCell> stockCells = new ArrayList<>(jsonArray.size());
        Map<String, List<StockCell>> monthGroupedStocks = new HashMap<>();
        for (int i = 0, size = jsonArray.size(); i < size; i++) {
            JSONObject stockObject = jsonArray.getJSONObject(i);
            String id = stockObject.getString("id");
            JSONObject cell = stockObject.getJSONObject("cell");
            String iporesult = cell.getString("iporesult");
            if (iporesult == null) {
                continue;
            }

            String stockName = cell.getString("stock_nm");
            // 2021-02-19
            String listDt2 = cell.getString("list_dt2");

            double luckyDrawRt = cell.getDouble("lucky_draw_rt");

            double singleDrawMoney = cell.getDouble("single_draw_money");

            double grayIncrRt = cell.getDouble("gray_incr_rt");

            double firstIncrRt = cell.getDouble("first_incr_rt");

            StockCell stockCell = new StockCell();
            stockCell.setId(id);
            stockCell.setName(stockName);
            stockCell.setDate(listDt2);
            stockCell.setSingleDrawMoney(singleDrawMoney);
            stockCell.setLuckyDrawRt(luckyDrawRt);
            stockCell.setGrayIncrRt(grayIncrRt);
            stockCell.setFirstIncrRt(firstIncrRt);

            List<StockCell> cells = monthGroupedStocks.computeIfAbsent(stockCell.getMonth(), k -> new ArrayList());
            cells.add(stockCell);

            stockCells.add(stockCell);
        }


        Map<String, Integer> numsStockByMonth = new HashMap<>();
        Map<String, Statistical> statisticalMap = new HashMap<>();

        for (Map.Entry<String, List<StockCell>> entry : monthGroupedStocks.entrySet()) {
            numsStockByMonth.put(entry.getKey(), entry.getValue().size());
//            System.out.println(entry.getKey() + " " + entry.getValue().size());
            Statistical s = runStatistical(entry.getValue());
//            System.out.println(entry.getKey() + " " + s.format());

            statisticalMap.put(entry.getKey(), s);
        }

//        System.out.println("#####################");
        Statistical s = runStatistical(stockCells);
        System.out.println(s.getAvgSingleDrawMoney());
//        System.out.println(s.format());

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dirPath = "output/" + dateFormat.format(new Date(System.currentTimeMillis()));
        File dir = new File(dirPath);
        dir.mkdirs();
        FileOutputStream fileWriter = new FileOutputStream(dirPath + "/nums.txt");
        for (Map.Entry<String, Integer> entry : numsStockByMonth.entrySet()) {
            writeString(fileWriter, entry.getKey() + "," + entry.getValue());
        }
        fileWriter.close();


        fileWriter = new FileOutputStream(dirPath + "/months.txt");
        writeString(fileWriter, "month, " + Statistical.header());
        for (Map.Entry<String, Statistical> entry : statisticalMap.entrySet()) {
            writeString(fileWriter, entry.getKey() + "," + entry.getValue().format());
        }
        fileWriter.close();

        fileWriter = new FileOutputStream(dirPath + "/total.txt");
        writeString(fileWriter, Statistical.header());
        writeString(fileWriter, s.format());
        fileWriter.close();

    }

    private static void writeString(FileOutputStream fileWriter, String value) throws IOException {
        fileWriter.write((value + "\n").getBytes(StandardCharsets.UTF_8));
    }

    private static Statistical runStatistical(List<StockCell> stockCells) {
        Statistical statistical = new Statistical();
        double totalLuckyDrawRt = stockCells.stream().mapToDouble(e -> e.getLuckyDrawRt()).sum();
        double avgLockyDrawRt = totalLuckyDrawRt / stockCells.size();

        long numIncrStocks = stockCells.stream().filter(e -> e.getFirstIncrRt() > 0).count();

        long numDescStocks = stockCells.stream().filter(e -> e.getFirstIncrRt() < 0).count();

        long numEqualStocks = stockCells.stream().filter(e -> Math.abs(e.getFirstIncrRt() - 0.000001) < 0).count();

        KeyValue minGrayIncRt = min(stockCells, stockCell -> stockCell.getGrayIncrRt());
        statistical.setMinGrayIncRt(minGrayIncRt);

        KeyValue minFirstIncRt = min(stockCells, stockCell -> stockCell.getFirstIncrRt());
        statistical.setMinFirstIncRt(minFirstIncRt);

        KeyValue maxGrayIncRt = max(stockCells, stockCell -> stockCell.getGrayIncrRt());
        statistical.setMaxGrayIncRt(maxGrayIncRt);

        KeyValue maxFirstIncRt = max(stockCells, stockCell -> stockCell.getFirstIncrRt());
        statistical.setMaxFirstIncRt(maxFirstIncRt);

        KeyValue minGrayIncMoney = min(stockCells, stockCell -> stockCell.getGrayIncrRt() * stockCell.getSingleDrawMoney() * 0.01);
        statistical.setMinGrayIncMoney(minGrayIncMoney);

        KeyValue minFirstIncMoney = min(stockCells, stockCell -> stockCell.getFirstIncrRt() * stockCell.getSingleDrawMoney() * 0.01);
        statistical.setMinFirstIncMoney(minFirstIncMoney);

        KeyValue maxGrayIncMoney = max(stockCells, stockCell -> stockCell.getGrayIncrRt() * stockCell.getSingleDrawMoney() * 0.01);
        statistical.setMaxGrayIncMoney(maxGrayIncMoney);

        KeyValue maxFirstIncMoney = max(stockCells, stockCell -> stockCell.getFirstIncrRt() * stockCell.getSingleDrawMoney()* 0.01);
        statistical.setMaxFirstIncMoney(maxFirstIncMoney);

        double grayTotal = 0;
        double firtTotal = 0;
        double sumSingleDrawMoney = 0;
        for (StockCell cell : stockCells) {
            double singleDrawMoney = cell.getSingleDrawMoney();
            sumSingleDrawMoney += singleDrawMoney;
            double grayIncrRt = cell.getGrayIncrRt();
            double firstIncrRt = cell.getFirstIncrRt();
            grayTotal += grayIncrRt * singleDrawMoney;
            firtTotal += firstIncrRt * singleDrawMoney;
        }

        statistical.setAvgLockyDrawRt(avgLockyDrawRt);
        statistical.setNumIncrStocks(numIncrStocks);
        statistical.setNumDescStocks(numDescStocks);
        statistical.setNumEqualStocks(numEqualStocks);

        statistical.setAvgSingleDrawMoney(sumSingleDrawMoney / stockCells.size());

        statistical.setGrayTotal(grayTotal * 0.01);

        statistical.setFirtTotal(firtTotal * 0.01);

        return statistical;
    }

    public static class KeyValue {
        private double value;
        private String name;

        public KeyValue(String name, double value) {
            this.value = value;
            this.name = name;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static KeyValue min(List<StockCell> stockCells, Function<StockCell, Double> fn) {
        String name = "";
        double min = Double.MAX_VALUE;
        for (StockCell stockCell : stockCells) {
            double value = fn.apply(stockCell);
            if (min > value) {
                min = value;
                name = stockCell.getName();
            }
        }
        return new KeyValue(name, min);
    }

    public static KeyValue max(List<StockCell> stockCells, Function<StockCell, Double> fn) {
        String name = "";
        double max = Double.MIN_VALUE;
        for (StockCell stockCell : stockCells) {
            double value = fn.apply(stockCell);
            if (max < value) {
                max = value;
                name = stockCell.getName();
            }
        }
        return new KeyValue(name, max);
    }


    static class Statistical {
        double avgLockyDrawRt;

        long numIncrStocks;

        long numDescStocks;

        long numEqualStocks;

        double grayTotal = 0;
        double firtTotal = 0;

        double avgSingleDrawMoney;

        KeyValue minGrayIncRt;

        KeyValue minFirstIncRt;

        KeyValue maxGrayIncRt;

        KeyValue maxFirstIncRt;

        KeyValue minGrayIncMoney;

        KeyValue minFirstIncMoney;

        KeyValue maxGrayIncMoney;

        KeyValue maxFirstIncMoney;

        KeyValue[] keyValues = new KeyValue[8];

        public double getAvgLockyDrawRt() {
            return avgLockyDrawRt;
        }

        public void setAvgLockyDrawRt(double avgLockyDrawRt) {
            this.avgLockyDrawRt = avgLockyDrawRt;
        }

        public long getNumIncrStocks() {
            return numIncrStocks;
        }

        public void setNumIncrStocks(long numIncrStocks) {
            this.numIncrStocks = numIncrStocks;
        }

        public long getNumDescStocks() {
            return numDescStocks;
        }

        public void setNumDescStocks(long numDescStocks) {
            this.numDescStocks = numDescStocks;
        }

        public long getNumEqualStocks() {
            return numEqualStocks;
        }

        public void setNumEqualStocks(long numEqualStocks) {
            this.numEqualStocks = numEqualStocks;
        }

        public double getGrayTotal() {
            return grayTotal;
        }

        public void setGrayTotal(double grayTotal) {
            this.grayTotal = grayTotal;
        }

        public double getFirtTotal() {
            return firtTotal;
        }

        public void setFirtTotal(double firtTotal) {
            this.firtTotal = firtTotal;
        }

        public KeyValue getMinGrayIncRt() {
            return minGrayIncRt;
        }

        public double getAvgSingleDrawMoney() {
            return avgSingleDrawMoney;
        }

        public void setAvgSingleDrawMoney(double avgSingleDrawMoney) {
            this.avgSingleDrawMoney = avgSingleDrawMoney;
        }

        public void setMinGrayIncRt(KeyValue minGrayIncRt) {
            keyValues[0] = minGrayIncRt;
            this.minGrayIncRt = minGrayIncRt;
        }

        public KeyValue getMinFirstIncRt() {
            return minFirstIncRt;
        }

        public void setMinFirstIncRt(KeyValue minFirstIncRt) {
            keyValues[1] = minFirstIncRt;
            this.minFirstIncRt = minFirstIncRt;
        }

        public KeyValue getMaxGrayIncRt() {
            return maxGrayIncRt;
        }

        public void setMaxGrayIncRt(KeyValue maxGrayIncRt) {
            keyValues[2] = maxGrayIncRt;
            this.maxGrayIncRt = maxGrayIncRt;
        }

        public KeyValue getMaxFirstIncRt() {
            return maxFirstIncRt;
        }

        public void setMaxFirstIncRt(KeyValue maxFirstIncRt) {
            keyValues[3] = maxFirstIncRt;
            this.maxFirstIncRt = maxFirstIncRt;
        }

        public KeyValue getMinGrayIncMoney() {
            return minGrayIncMoney;
        }

        public void setMinGrayIncMoney(KeyValue minGrayIncMoney) {
            keyValues[4] = minGrayIncMoney;
            this.minGrayIncMoney = minGrayIncMoney;
        }

        public KeyValue getMinFirstIncMoney() {
            return minFirstIncMoney;
        }

        public void setMinFirstIncMoney(KeyValue minFirstIncMoney) {
            keyValues[5] = minFirstIncMoney;
            this.minFirstIncMoney = minFirstIncMoney;
        }

        public KeyValue getMaxGrayIncMoney() {
            return maxGrayIncMoney;
        }

        public void setMaxGrayIncMoney(KeyValue maxGrayIncMoney) {
            keyValues[6] = maxGrayIncMoney;
            this.maxGrayIncMoney = maxGrayIncMoney;
        }

        public KeyValue getMaxFirstIncMoney() {
            return maxFirstIncMoney;
        }

        public void setMaxFirstIncMoney(KeyValue maxFirstIncMoney) {
            keyValues[7] = maxFirstIncMoney;
            this.maxFirstIncMoney = maxFirstIncMoney;
        }

        private static String format = "%.2f, %d, %d, %d, %.2f, %.2f, %.2f";

        static {
            for (int i = 0; i < 8; i++) {
                format += ", %s, %.2f";
            }
        }

        public static String header() {
            StringBuilder builder = new StringBuilder();
            builder.append("avgLockyDrawRt").append(",");

            builder.append("numIncrStocks").append(",");

            builder.append("numDescStocks").append(",");

            builder.append("numEqualStocks").append(",");

            builder.append("desc/total").append(",");

            builder.append("grayTotal").append(",");
            builder.append("firtTotal").append(",");

            builder.append("minGrayIncRt.name").append(",");
            builder.append("minGrayIncRt.value").append(",");

            builder.append("minFirstIncRt.name").append(",");
            builder.append("minFirstIncRt.value").append(",");

            builder.append("maxGrayIncRt.name").append(",");
            builder.append("maxGrayIncRt.value").append(",");

            builder.append("maxFirstIncRt.name").append(",");
            builder.append("maxFirstIncRt.value").append(",");

            builder.append("minGrayIncMoney.name").append(",");
            builder.append("minGrayIncMoney.value").append(",");

            builder.append("minFirstIncMoney.name").append(",");
            builder.append("minFirstIncMoney.value").append(",");

            builder.append("maxGrayIncMoney.name").append(",");
            builder.append("maxGrayIncMoney.value").append(",");

            builder.append("maxFirstIncMoney.name").append(",");
            builder.append("maxFirstIncMoney.value");

            return builder.toString();
        }

        public String format() {
            Object[] objects = new Object[8 * 2 + 7];
            objects[0] = avgLockyDrawRt;
            objects[1] = numIncrStocks;
            objects[2] = numDescStocks;
            objects[3] = numEqualStocks;
            objects[4] = numDescStocks * 1.0 / (numDescStocks + numIncrStocks + numEqualStocks);
            objects[5] = grayTotal;
            objects[6] = firtTotal;
            int idx = 7;
            for (int i = 0; i < keyValues.length; i++) {
                objects[idx] = keyValues[i].name;
                idx++;
                objects[idx] = keyValues[i].value;
                idx++;
            }
            return String.format(format, objects);
        }
    }

    static class StockCell {
        private String id;
        private String name;
        private String month;
        private String year;
        private String date;

        private double luckyDrawRt;

        private double singleDrawMoney;

        private double grayIncrRt;

        private double firstIncrRt;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getMonth() {
            return month;
        }

        public String getYear() {
            return year;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
            this.year = date.substring(0, date.indexOf("-"));
            this.month = date.substring(0, date.lastIndexOf("-"));
        }

        public double getLuckyDrawRt() {
            return luckyDrawRt;
        }

        public void setLuckyDrawRt(double luckyDrawRt) {
            this.luckyDrawRt = luckyDrawRt;
        }

        public double getSingleDrawMoney() {
            return singleDrawMoney;
        }

        public void setSingleDrawMoney(double singleDrawMoney) {
            this.singleDrawMoney = singleDrawMoney;
        }

        public double getGrayIncrRt() {
            return grayIncrRt;
        }

        public void setGrayIncrRt(double grayIncrRt) {
            this.grayIncrRt = grayIncrRt;
        }

        public double getFirstIncrRt() {
            return firstIncrRt;
        }

        public void setFirstIncrRt(double firstIncrRt) {
            this.firstIncrRt = firstIncrRt;
        }
    }
}
