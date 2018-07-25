import com.beust.jcommander.JCommander;
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

        Setting.CONFIG_PATH = args.configPath.length() == 0 ? Setting.getEtc("river.json").getAbsolutePath() : args.configPath;

        try {
            River river = Setting.readRiver();

        } catch (Exception e)
        {
            e.printStackTrace();
            System.out.println(e);
        }
    }
}
