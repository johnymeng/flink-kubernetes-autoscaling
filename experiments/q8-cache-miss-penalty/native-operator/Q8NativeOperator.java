import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.kubernetes.operator.FlinkOperator;

public final class Q8NativeOperator {
    public static void main(String[] args) throws Exception {
        Map<String, String> entries = new LinkedHashMap<>();
        entries.put("org.apache.flink.autoscaler.ScalingMetricEvaluator", "variant.jar");
        entries.put("org.apache.flink.autoscaler.RestApiMetricsCollector", "variant.jar");
        entries.put("org.apache.flink.autoscaler.JobAutoScalerImpl", "variant.jar");
        entries.put("org.apache.flink.autoscaler.a4s.A4S", "variant.jar");
        entries.put("org.apache.flink.a4s.core.MissRateCurve", "telemetry.jar");
        entries.put("org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFactory", "wiring.jar");
        entries.put("org.apache.flink.kubernetes.operator.autoscaler.KubernetesScalingRealizer", "wiring.jar");
        entries.put("org.apache.flink.kubernetes.operator.service.NativeFlinkService", "wiring.jar");
        for (Map.Entry<String,String> row : entries.entrySet()) {
            Class<?> cls = Class.forName(row.getKey(), false, Q8NativeOperator.class.getClassLoader());
            cls.getDeclaredMethods();
            Path path = Path.of(cls.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!row.getValue().equals(path.getFileName().toString())) {
                throw new IllegalStateException("Unexpected source of " + cls.getName() + " at " + path);
            }
            System.out.println("Q8_NATIVE_OPERATOR " + cls.getName() + " " + path);
        }
        if (args.length == 1 && args[0].equals("--verify-only")) return;
        FlinkOperator.main(args);
    }
}
