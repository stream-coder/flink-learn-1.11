import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author GangW
 */
public class Log4j2PrintLog {
    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(Log4j2PrintLog.class);

        log.info("test");

        System.out.println("finished");
    }
}
