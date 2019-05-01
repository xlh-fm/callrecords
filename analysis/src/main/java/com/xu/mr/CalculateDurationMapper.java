package com.xu.mr;

import com.xu.kv.DateDimension;
import com.xu.kv.UnionDimension;
import com.xu.kv.UserDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

//context.write(08012345678_2018-06-01,1234)
public class CalculateDurationMapper extends TableMapper<UnionDimension, Text> {
    private UnionDimension unionDimension = new UnionDimension();
    private Text v = new Text();

    private Map<String,String> user = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        user = new HashMap<>();
        user.put("08015410161","Allen");
        user.put("08038756032","Ben");
        user.put("08094712037","Carl");
        user.put("07019340019","Dunn");
        user.put("07011199562","Emma");
        user.put("08095640023","Franklin");
        user.put("08077609908","Gray");
        user.put("07076654310","Harris");
        user.put("07045999121","Isabella");
        user.put("07042022192","Jack");
        user.put("09041355345","King");
        user.put("09018677786","Larry");
        user.put("08022206819","Marry");
        user.put("07012643422","Noel");
        user.put("07034985734","Olivia");
        user.put("09078346578","Porter");
        user.put("09023678423","Quintina");
        user.put("09034578929","Russell");
        user.put("09078923434","Steven");
        user.put("08034589734","Tony");
        user.put("07032091728","Ulysses");
        user.put("08004651222","Vera");
        user.put("09004568050","Wilson");
        user.put("08084762621","Xavier");
        user.put("07099001965","Yolanda");
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        //rowkey: 0X_080XXXXXXXX_yyyy-MM-dd HH:mm:ss_timestamp_070XXXXXXXX_flag_duration
        String rowKey = Bytes.toString(key.get());
        String[] splits = rowKey.split("_");
        //do not use data in f2 column family
        if (splits[5].equals("2")) {
            return;
        }

        String callingNumber = splits[1];
        String setupTime = splits[2];
        String calledNumber = splits[4];
        String duration = splits[6];

        String year = setupTime.substring(0, 4);
        String month = setupTime.substring(5, 7);
        String day = setupTime.substring(8, 10);

        v.set(duration);

        //calling user dimension
        UserDimension callingDimension = new UserDimension(callingNumber,user.get(callingNumber));
        unionDimension.setUserDimension(callingDimension);

        //year dimension
        DateDimension yearDimension = new DateDimension(year, "-1", "-1");
        unionDimension.setDateDimension(yearDimension);

        context.write(unionDimension,v);

        //month dimension
        DateDimension monthDimension = new DateDimension(year, month, "-1");
        unionDimension.setDateDimension(monthDimension);

        context.write(unionDimension,v);

        //day dimension
        DateDimension dayDimension = new DateDimension(year, month, day);
        unionDimension.setDateDimension(dayDimension);

        context.write(unionDimension,v);

        //called user dimension
        UserDimension calledDimension = new UserDimension(calledNumber,user.get(calledNumber));
        unionDimension.setUserDimension(calledDimension);

        //year dimension
        unionDimension.setDateDimension(yearDimension);

        context.write(unionDimension,v);

        //month dimension
        unionDimension.setDateDimension(monthDimension);

        context.write(unionDimension,v);

        //day dimension
        unionDimension.setDateDimension(dayDimension);

        context.write(unionDimension,v);
    }
}
