package com.xu;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class ProduceLog {
    private static ArrayList<String> phoneList = null;

    static {
        phoneList = new ArrayList<>();

        phoneList.add("08015410161");
        phoneList.add("08038756032");
        phoneList.add("08094712037");
        phoneList.add("07019340019");
        phoneList.add("07011199562");
        phoneList.add("08095640023");
        phoneList.add("08077609908");
        phoneList.add("07076654310");
        phoneList.add("07045999121");
        phoneList.add("07042022192");
        phoneList.add("09041355345");
        phoneList.add("09018677786");
        phoneList.add("08022206819");
        phoneList.add("07012643422");
        phoneList.add("07034985734");
        phoneList.add("09078346578");
        phoneList.add("09023678423");
        phoneList.add("09034578929");
        phoneList.add("09078923434");
        phoneList.add("08034589734");
        phoneList.add("07032091728");
        phoneList.add("08004651222");
        phoneList.add("09004568050");
        phoneList.add("08084762621");
        phoneList.add("07099001965");
    }

    //Randomly generate call setup time
    private static long callSetupTime(String firstDate, String secondDate) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date start = simpleDateFormat.parse(firstDate);
        Date end = simpleDateFormat.parse(secondDate);

        long setupTime = (long) (Math.random() * (end.getTime() - start.getTime())) + start.getTime();
        return setupTime;
    }

    //produce log
    private static String produceLog() throws ParseException, InterruptedException {
        //get two phone numbers
        int index1 = new Random().nextInt(phoneList.size());
        int index2 = -1;

        String phone1 = phoneList.get(index1);
        String phone2 = null;
        while (true) {
            index2 = new Random().nextInt(phoneList.size());
            phone2 = phoneList.get(index2);
            if (!phone1.equals(phone2)) break;
        }
        //Randomly generate call duration
        int callDuration = new Random().nextInt(3600) + 1;

        //format call duration
        String duration = new DecimalFormat("0000").format(callDuration);

        //get call setup time
        long setupTime = callSetupTime("2018-01-01", "2019-01-01");
        String setupTimeString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(setupTime);

        //produce log
        StringBuilder log = new StringBuilder();

        log.append(phone1 + ",")
                .append(phone2 + ",")
                .append(setupTimeString + ",")
                .append(duration);
        Thread.sleep(250);

        return log.toString();
    }

    //write log
    public static void writeLog(String filePath) {
        OutputStreamWriter outputStreamWriter = null;
        try {
            outputStreamWriter = new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8");
            while (true) {
                String log = ProduceLog.produceLog();
                outputStreamWriter.write(log + "\n");
                outputStreamWriter.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                outputStreamWriter.flush();
                outputStreamWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        if (args == null || args.length <= 0) {
            System.out.println("no arguments");
            System.exit(1);
        }
        ProduceLog.writeLog(args[0]);
    }
}