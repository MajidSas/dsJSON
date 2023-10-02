
import java.io.*;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static com.jayway.jsonpath.JsonPath.parse;

public class Main {

    private static String readFile(String file) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), "UTF-8"));
        String         line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String         ls = System.getProperty("line.separator");

        try {
            while((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }

            return stringBuilder.toString();
        } finally {
            reader.close();
        }
    }

    public static void main(String[] args) throws IOException {
        String filePath = args[0];
        String pathQuery = args[1];

        File f = new File(filePath);
        String[] paths = { filePath };
        if(f.isDirectory()) {
            paths = f.list();
        }
        long totalTime = 0L;
        long totalSize = 0L;
        for(String file : paths) {
            if(f.isDirectory()) {
                file = filePath + file;
            }
            long start = System.currentTimeMillis();
            InputStream input = new BufferedInputStream(
                    new FileInputStream(file));
            List<Map<String, Object>> products = parse(input).read(pathQuery);
            long end =  System.currentTimeMillis();
            totalTime += end-start;
            totalSize += products.size();
        }

        System.out.println("Records found:  " + totalSize +
                "\nTotal Time: " + totalTime);
    }
}
