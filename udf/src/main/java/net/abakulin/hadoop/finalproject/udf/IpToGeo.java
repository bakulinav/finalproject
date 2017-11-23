package net.abakulin.hadoop.finalproject.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;

public class IpToGeo extends UDF implements Serializable {

    private static final Logger log = Logger.getLogger(IpToGeo.class);

    // network,low,high,low_num,high_num,geoname_id
    private String dataPath = "/ips.csv";
    ArrayList<Pair<Pair<Long, Long>, Integer>> ipData;

    public IpToGeo() throws FileNotFoundException {
        ipData = getIpData();
        log.info("resolver initialized");
        log.info(ipData.size() + " ips loaded");
    }

    public Integer evaluate(String ip) {
        return getCountryByIp(ipData, ipToNum(ip));
    }
    
    private Integer getCountryByIp(ArrayList<Pair<Pair<Long, Long>, Integer>> ipRangeToCountry, Long ip) {
        log.info("binary search of "+ip);
        return binarySearch(ipRangeToCountry, ip, 0, ipRangeToCountry.size() - 1);
    }

    private Integer binarySearch(ArrayList<Pair<Pair<Long, Long>, Integer>> ipRangeToCountry, Long ip, Integer start, Integer end) {
        if (start > end) return 0;

        Integer mid = start + (end-start+1)/2;

        Pair<Pair<Long, Long>, Integer> lhc = ipRangeToCountry.get(mid); // low-high-country
        switch(inRange(ip, lhc._1)) {
            case 0: return lhc._2;
            case -1: return binarySearch(ipRangeToCountry, ip, start, mid - 1);
            case 1: return binarySearch(ipRangeToCountry, ip, mid + 1, end);
        }

        return 0;
    }

    private int inRange(Long ip, Pair<Long, Long> rng) {
        if (rng._1 <= ip && ip <= rng._2) return  0;
        else if (ip < rng._1) return  -1;
        else if (ip > rng._2) return 1;
        else return -1;
    }

    private Long ipToNum(String ip) {
        String[] arr = ip.split("\\.");

        return (Long.valueOf(arr[0]) << 24) +
                (Long.valueOf(arr[1]) << 16) +
                (Long.valueOf(arr[2]) << 8) +
                Long.valueOf(arr[3]);
    }

    private ArrayList<Pair<Pair<Long, Long>, Integer>> getIpData() throws FileNotFoundException {
        log.info("getting data from file");
        ArrayList<Pair<Pair<Long, Long>, Integer>> out = new ArrayList<Pair<Pair<Long, Long>, Integer>>();
        InputStream ipStream = getClass().getResourceAsStream(dataPath);

        if (ipStream == null) throw new FileNotFoundException("File " + dataPath + " was not found in source package");

        try {


            BufferedReader bufferedSource = new BufferedReader(new InputStreamReader(ipStream));
            bufferedSource.readLine(); // skip header

            String ln;
            String[] arr;
            while ((ln = bufferedSource.readLine()) != null) {
                arr = ln.split(",");
                out.add(
                        new Pair(
                                new Pair(Long.valueOf(arr[3]), Long.valueOf(arr[4])),
                                Integer.valueOf(arr[5]))
                );
            }

            bufferedSource.close();

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return out;
    }

    public static void main(String[] args) {
        String[] s = new String[]{"hello","world"};
        System.out.println(String.format("%s %s", s));
    }

}
