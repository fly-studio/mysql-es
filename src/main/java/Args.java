import com.beust.jcommander.Parameter;

public class Args {
    @Parameter(names = { "--config", "-c"}, description = "Path of the config's directory.")
    public String etcPath = "";
}
