package constants;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class Args {
    @Parameter
    public List<String> parameters = new ArrayList<>();

    @Parameter(names = {"-Topic", "name"}, description = "Main topic to subscribe too")
    public String Topic = "T1";

    @Parameter(names = {"-TransactionID", "ID"}, description = "TransactionID needs to be unique")
    public String TransactionID = "default";

    @Parameter(names = {"-GroupID", "GroupID"}, description = "GroupID for consumer")
    public String GroupID = "-1";

    @Parameter(names = "-ID", description = "Client ID")
    public Integer ID = 100;

    @Parameter(names = "-debug", description = "Debug mode")
    public boolean debug = false;
}
