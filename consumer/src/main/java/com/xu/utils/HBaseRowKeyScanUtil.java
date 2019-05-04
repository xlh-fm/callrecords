package com.xu.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

//generate multiple groups of startRow and stopRow
public class HBaseRowKeyScanUtil {
    private static List<String[]> list;

    //09078923434,2018,04-06
    public static List<String[]> getStartStop(String phone, String start, String stop) throws ParseException {
        list = new ArrayList<>();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
        Date startDate = dateFormat.parse(start);
        Date stopDate = dateFormat.parse(stop);

        Calendar startPoint = Calendar.getInstance();
        startPoint.setTime(startDate);

        Calendar stopPoint = Calendar.getInstance();
        stopPoint.setTime(startDate);
        stopPoint.add(Calendar.MONTH, 1);

        while (startPoint.getTimeInMillis() <= stopDate.getTime()) {
            String setupTime = dateFormat.format(startPoint.getTime());
            String partition = HBaseUtil.getPartition(Integer.valueOf(ResourcesUtil.getProperties().getProperty
                            ("hbase.regions")), phone,
                    setupTime);
            String startRow = partition + "_" + phone + "_" + setupTime;
            String stopRow = partition + "_" + phone + "_" + dateFormat.format(stopPoint.getTime());

            list.add(new String[]{startRow, stopRow});

            startPoint.add(Calendar.MONTH, 1);
            stopPoint.add(Calendar.MONTH, 1);

        }
        return list;
    }
}
