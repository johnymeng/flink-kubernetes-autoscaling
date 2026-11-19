import java.nio.file.Path;

/** Print the loaded origins of the production and benchmark classes on the supplied JVM classpath. */
public class ClasspathProof {
    public static void main(String[] args) throws Exception {
        for (String name : new String[] {
                "org.apache.flink.autoscaler.RyanQ8Policy",
                "org.apache.flink.autoscaler.ScalingMetricEvaluator",
                "org.apache.flink.autoscaler.ScalingExecutor",
                "org.apache.flink.autoscaler.a4s.A4S",
                "org.apache.flink.autoscaler.a4s.OnlineCacheCost",
                "org.apache.flink.a4s.core.MissRateCurve",
                "org.apache.flink.a4s.core.StackDistanceHistogram"}) {
            if (args.length == 1 && args[0].equals("--legacy")
                    && name.equals("org.apache.flink.autoscaler.a4s.OnlineCacheCost")) {
                continue;
            }
            Class<?> loaded = Class.forName(name, false, ClasspathProof.class.getClassLoader());
            Path origin = Path.of(loaded.getProtectionDomain().getCodeSource().getLocation().toURI());
            System.out.println(name + "\t" + origin.toRealPath());
        }
    }
}
