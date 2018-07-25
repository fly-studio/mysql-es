import com.beust.jcommander.JCommander;
import com.fly.sync.setting.Config;
import com.fly.sync.setting.River;
import com.fly.sync.setting.Setting;

public class Main {

    public static Args args;

    public static void main(String[] argv) {
        args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);

        Setting.ETC_PATH = args.etcPath.length() == 0 ? Setting.getEtc().getAbsolutePath() : args.etcPath;

        try {


        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println(e);
        }
    }
}
