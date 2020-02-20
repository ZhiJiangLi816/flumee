import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/***
 * @author Zhi-jiang li
 * @date 2020/1/19 0019 14:49
 **/
public class TestDemo {
    public static void main(String[] args) throws ParseException {
        String date = "17/05/2015:10:05:03";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
        long parse = simpleDateFormat.parse(date).getTime();

        Date date1 = simpleDateFormat1.parse(date);
        String format = simpleDateFormat2.format(date1);

        System.out.println(format);
    }
}
