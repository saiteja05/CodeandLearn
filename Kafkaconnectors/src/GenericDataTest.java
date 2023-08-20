import org.apache.avro.shaded.logisland.Schema;
import org.apache.avro.shaded.logisland.generic.GenericData;
import org.apache.avro.shaded.logisland.generic.GenericRecord;

public class GenericDataTest {

    static String userSchema = "{\"type\":\"record\"," +
            "\"name\":\"Car\"," +
            "\"fields\":[{\"name\":\"brand\",\"type\":\"string\"}," +
            "{\"name\":\"horsepower\",\"type\":\"int\",\"default\":-1}]}";
    static Schema s = new Schema.Parser().parse(userSchema);


    public static void main(String[] args) {

        int max = 15;

        String brand = "BMWSeries"+Math.ceil(Math.random()*max);
        int hp = (int) (100*Math.random()*max);

        GenericRecord avroRecord = new GenericData.Record(s);
        avroRecord.put("brand", brand);
        avroRecord.put("horsepower", hp);

        System.out.println(avroRecord.get("brand"));
        System.out.println(avroRecord.get("horsepower"));

    }


}
