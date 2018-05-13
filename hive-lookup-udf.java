package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class GetDefaultPopulation extends UDF {
    private Map<String, Integer> populationMap;

    public Integer evaluate(String sizeType, String mapFile) throws HiveException {
        if (populationMap == null) {
            populationMap = new HashMap<String, Integer>();
            try {
                BufferedReader lineReader = new BufferedReader(new FileReader(mapFile));

                String line = null;
                while ((line = lineReader.readLine()) != null) {
                    String[] pair = line.split("\t");
                    String type = pair[0];
                    int population = Integer.parseInt(pair[1]);
                    populationMap.put(type, population);
                }
            } catch (FileNotFoundException e) {
                throw new HiveException(mapFile + " doesn't exist");
            } catch (IOException e) {
                throw new HiveException("process file " + mapFile + " failed, please check format");
            }
        }

        if (populationMap.containsKey(sizeType)) {
            return populationMap.get(sizeType);
        }

        return null;
    }
}
