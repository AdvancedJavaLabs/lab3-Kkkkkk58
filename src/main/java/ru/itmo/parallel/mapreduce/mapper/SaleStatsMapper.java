package ru.itmo.parallel.mapreduce.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.itmo.parallel.mapreduce.writable.SalesWritable;

public class SaleStatsMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(SaleStatsMapper.class);

    private static final String CSV_DELIMITER = ",";
    private static final int CATEGORY_INDEX = 2;
    private static final int PRICE_INDEX = 3;
    private static final int QUANTITY_INDEX = 4;
    private static final int MIN_COLUMNS = 5;

    private final Text categoryKey = new Text();
    private final SalesWritable salesValue = new SalesWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        if (line.isEmpty()) {
            LOG.debug("[SALE_STATS] Skipping empty line");
            return;
        }
        if (line.startsWith("transaction_id")) {
            LOG.debug("[SALE_STATS] Skipping csv header: {}", line);
            return;
        }

        String[] columns = line.split(CSV_DELIMITER);
        
        if (columns.length < MIN_COLUMNS) {
            LOG.warn("[SALE_STATS] Line is malformed and contains only {} columns: {}", columns.length, line);
            return;
        }

        try {
            String category = columns[CATEGORY_INDEX].trim();
            double price = Double.parseDouble(columns[PRICE_INDEX].trim());
            long quantity = Long.parseLong(columns[QUANTITY_INDEX].trim());

            double revenue = price * quantity;

            salesValue.setRevenue(revenue);
            salesValue.setQuantity(quantity);

            categoryKey.set(category);
            context.write(categoryKey, salesValue);
        } catch (NumberFormatException e) {
            LOG.error("[SALE_STATS] Failed to parse line '{}'. Reason: {}", line, e.getMessage(), e);
        }
    }
}
