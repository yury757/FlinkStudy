package net.yury.processfunction;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.yury.serialize.DBDeserialization;
import net.yury.utils.DateUtil8;
import net.yury.utils.DruidUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.postgresql.PGConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FundProcessFunction extends ProcessFunction<ObjectNode, ObjectNode> {
    public static final String s1 = "select fund_code, sum(net_value) / count(net_value) from fund_quote where fund_code = '#{fund_code}' and date between '#{startDate}'::date and '#{endDate}'::date group by fund_code";
    public static final String s2 = "insert into fund_interval_quote (fund_code, \"date\", interval_type, net_value) values (?,?,?,?) on conflict (fund_code, \"date\", interval_type) do update set net_value = excluded.net_value";
    private PGConnection pgConnection;

    @Override
    public void processElement(ObjectNode value, Context ctx, Collector<ObjectNode> out) throws Exception {
        String op = value.get("op").textValue();
        List<Fund> fundList = new ArrayList<>(2);
        if (DBDeserialization.INSERT.equals(op)) {
            ObjectNode after = (ObjectNode)value.get(DBDeserialization.AFTER);
            Fund fund = new Fund();
            fund.setFundCode(after.get("fund_code").textValue());
            fund.setDate(DateUtil8.parseDateStr(after.get("date").textValue()));
            fundList.add(fund);
        }else if (DBDeserialization.DELETE.equals(op)) {
            ObjectNode before = (ObjectNode)value.get(DBDeserialization.BEFORE);
            Fund fund = new Fund();
            fund.setFundCode(before.get("fund_code").textValue());
            fund.setDate(DateUtil8.parseDateStr(before.get("date").textValue()));
            fundList.add(fund);
        }else if (DBDeserialization.UPDATE.equals(op)) {
            ObjectNode after = (ObjectNode)value.get(DBDeserialization.AFTER);
            ObjectNode before = (ObjectNode)value.get(DBDeserialization.BEFORE);
            ArrayNode diffFields = (ArrayNode)value.get("diffField");
            Iterator<JsonNode> iterator = diffFields.iterator();
            Fund fund = new Fund();
            fund.setFundCode(after.get("fund_code").textValue());
            fund.setDate(DateUtil8.parseDateStr(after.get("date").textValue()));
            fundList.add(fund);
            while (iterator.hasNext()) {
                String next = iterator.next().textValue();
                if ("fund_code".equals(next) || "date".equals(next)) {
                    Fund fund2 = new Fund();
                    fund2.setFundCode(before.get("fund_code").textValue());
                    fund2.setDate(DateUtil8.parseDateStr(before.get("date").textValue()));
                    fundList.add(fund2);
                    break;
                }
            }
        }
        processFundList(fundList);
        out.collect(value);
    }

    public void processFundList(List<Fund> fundList) throws SQLException {
        processWeekAvg(fundList);
        processMonthAvg(fundList);
        processYearAvg(fundList);
    }

    public void processWeekAvg(List<Fund> fundList) throws SQLException {
        for (Fund fund : fundList) {
            String fundCode = fund.getFundCode();
            LocalDate date = fund.getDate();
            LocalDate weekStart = DateUtil8.getWeekStart(date);
            LocalDate WeekEnd = DateUtil8.getWeekEnd(date);
            Double avg = getAvgNetValue(fundCode, weekStart, WeekEnd);
            insertAvgNetValue(fundCode, weekStart, 2, avg);
        }
    }

    public void processMonthAvg(List<Fund> fundList) throws SQLException {
        for (Fund fund : fundList) {
            String fundCode = fund.getFundCode();
            LocalDate date = fund.getDate();
            LocalDate weekStart = DateUtil8.getMonthStart(date);
            LocalDate WeekEnd = DateUtil8.getMonthEnd(date);
            Double avg = getAvgNetValue(fundCode, weekStart, WeekEnd);
            insertAvgNetValue(fundCode, weekStart, 3, avg);
        }
    }

    public void processYearAvg(List<Fund> fundList) throws SQLException {
        for (Fund fund : fundList) {
            String fundCode = fund.getFundCode();
            LocalDate date = fund.getDate();
            LocalDate weekStart = DateUtil8.getYearStart(date);
            LocalDate WeekEnd = DateUtil8.getYearEnd(date);
            Double avg = getAvgNetValue(fundCode, weekStart, WeekEnd);
            insertAvgNetValue(fundCode, weekStart, 4, avg);
        }
    }

    public Double getAvgNetValue(String fundCode, LocalDate startDate, LocalDate endDate) throws SQLException {
        String sql = s1
                .replace("#{fund_code}", fundCode)
                .replace("#{startDate}", DateUtil8.formatDateStr(startDate))
                .replace("#{endDate}", DateUtil8.formatDateStr(endDate));
        DruidPooledConnection connection = DruidUtil.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        Double d;
        if (resultSet.next()) {
            d = resultSet.getDouble(2);
        }else {
            d = null;
        }
        resultSet.close();
        statement.close();
        connection.recycle();
        return d;
    }

    public void insertAvgNetValue(String fundCode, LocalDate date, int intervalType, Double netValue) throws SQLException {
        DruidPooledConnection connection = DruidUtil.getConnection();
        PreparedStatement statement = connection.prepareStatement(s2);
        statement.setString(1, fundCode);
        statement.setDate(2, DateUtil8.toSqlDate(date));
        statement.setInt(3, intervalType);
        if (netValue == null) {
            statement.setNull(4, Types.DECIMAL);;
        }else {
            statement.setDouble(4, netValue);
        }
        statement.executeUpdate();
        statement.close();
        connection.recycle();
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class IntervalFund {
    private String fundCode;
    private LocalDate date;
    private int intervalType;
    private Double netValue;
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class Fund {
    private String fundCode;
    private LocalDate date;
    private Double netValue;
}