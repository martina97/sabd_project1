package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

public class ZoneMap {

    public static HashMap<Long, String> createZoneMap() throws IOException {

        URL url = new URL("https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv");
        URLConnection connection = url.openConnection();

        InputStreamReader input = new InputStreamReader(connection.getInputStream());
        BufferedReader buffer = null;

        buffer = new BufferedReader(input);
        buffer.readLine();
        String line =  null;

        HashMap<Long,String> map = new HashMap<Long, String>();

        while((line=buffer.readLine())!=null) {
            String[] str = line.split(",");
            String value = str[1].substring(1, str[1].length() - 1)+"/"+str[2].substring(1, str[2].length() - 1)+ "/"+str[3].substring(1, str[3].length() - 1);
            map.put(Long.valueOf(str[0]),value );
        }

        return map;


    }
}
