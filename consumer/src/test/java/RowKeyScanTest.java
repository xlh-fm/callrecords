import com.xu.utils.HBaseRowKeyScanUtil;
import com.xu.utils.HBaseUtil;
import com.xu.utils.ResourcesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class RowKeyScanTest {
    //09078923434,2018,04-06
    public static void main(String[] args) throws ParseException, IOException {

        //get every startrow and stoprow
        List<String[]> startStops = HBaseRowKeyScanUtil.getStartStop("09078923434", "2018-11", "2019-01");

        Connection connection = ConnectionFactory.createConnection(HBaseUtil.configuration);
        Table table = connection.getTable(TableName.valueOf(ResourcesUtil.getProperties().getProperty("hbase.table" +
                ".name")));

        for (String[] startStop : startStops) {
            Scan scan = new Scan(Bytes.toBytes(startStop[0]), Bytes.toBytes(startStop[1]));
            System.out.println(startStop[0]);
            System.out.println(startStop[1]);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
            }
        }
    }
}
