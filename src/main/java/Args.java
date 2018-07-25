import com.beust.jcommander.Parameter;

public class Args {
    @Parameter(names = { "--config", "-c"}, description = "Path to config")
    public String configPath = "";
}
