import com.xu.utils.HBaseFilterUtil;
import com.xu.utils.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * use filter to get target data
 */
public class FilterScanTest {
    //09078923434,2018,04-06
    public static void main(String[] args) throws IOException {
        Filter filter1 = HBaseFilterUtil.eqFilter("f1", "phone1", Bytes.toBytes("09078923434"));
        Filter filter2 = HBaseFilterUtil.eqFilter("f1", "phone2", Bytes.toBytes("09078923434"));
        Filter filter3 = HBaseFilterUtil.orFilter(filter1, filter2);

        Filter filter4 = HBaseFilterUtil.gteqFilter("f1", "setupTime", Bytes.toBytes("2018-04"));
        Filter filter5 = HBaseFilterUtil.ltFilter("f1", "setupTime", Bytes.toBytes("2018-07"));
        Filter filter6 = HBaseFilterUtil.andFilter(filter4, filter5);

        Filter filter7 = HBaseFilterUtil.andFilter(filter3, filter6);

        Scan scan = new Scan();
        scan.setFilter(filter7);
        Connection connection = ConnectionFactory.createConnection(HBaseUtil.configuration);
        Table table = connection.getTable(TableName.valueOf("call_project:callrecords"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey: " + Bytes.toString(CellUtil.cloneRow(cell)) + " cn: " + Bytes.toString
                        (CellUtil.cloneQualifier(cell)) + " value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
