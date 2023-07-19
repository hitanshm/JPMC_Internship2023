import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimeStampExample {

    // 2021.03.24.16.34.26
    //private static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    // 2021-03-24T16:44:39.083+08:00
    //private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    // 2021-03-24 16:48:05
    //private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {

        //String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
        //System.out.println(timeStamp);

        System.out.println(ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu.MM.dd.HH.mm.ss" ) ));

    }
}