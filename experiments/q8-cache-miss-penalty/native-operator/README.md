# Built-in operators for the Q8 comparison

Run one Flink Kubernetes Operator for each revision. Each watches only its arm’s namespace. A Q8 job runs under its own operator, which collects the metrics, invokes A4S and sends the native in-place resource update to Flink.

The runner also works with operators already deployed by your team. Give it the operator namespace, deployment, container, absolute path of the loaded autoscaler JAR, SHA-256 and source revision for each arm. Record startup class origins when the container has additional JARs containing autoscaler classes.

## Prepare an operator for each revision

Use a Kubernetes API with the Flink CRDs installed and two namespaces with disjoint controller ownership. A namespace under an existing cluster-wide Flink operator is already watched. The measured lab run used an isolated virtual Kubernetes API, the repository's A4S Helm chart, one controller per namespace, and a five-second operator reconciliation interval.

The helper requires Bash 4+, a JDK with `javac --release 11`, Python 3 with PyYAML, Helm 3 and Git. Supply:

- An immutable native A4S Flink operator image and its exact base source commit. It must contain the standard `/flink-kubernetes-operator` distribution and the Java 11 entry point for this operator. For the November 19 lab run, the main base image was `142.150.234.165:5000/flink-kubernetes-operator@sha256:8beccd2276917052a6626bb017f424d4a5288c3c10b4be416a3091a2fbefcfb6`, labelled with source revision `f8ce7fbbfae48869d8b1869748ad517801a9f726`.
- The shaded `flink-kubernetes-operator-1.11-SNAPSHOT-shaded.jar` from that image. The rendered pod hashes its own copy on startup. For example, use `docker create "$BASE_IMAGE"`, `docker cp` the JAR from the resulting container’s `/flink-kubernetes-operator/` directory, then remove the disposable container.
- The Flink distribution library directory, matching operator compile dependencies and Lombok JAR, and the common `abi3-mrc-telemetry.jar` from the experiment build. See the [experiment build guide](../BUILD.md). Both arms use the same telemetry ABI so the online policy can deserialize Q8's runtime metrics.

Run the following for each arm, supplying `REVISION` as a full commit and changing `ARM_NAMESPACE` and `OUT_DIR` between arms. Set `REPO` to the project checkout. Use the same base wherever the operator and operator API source trees match. The build checks this compatibility, archives the requested revision, and compiles the autoscaler and native operator wiring from its committed sources.

```bash
HELPER="$REPO/experiments/q8-cache-miss-penalty/native-operator"
bash "$HELPER/build.sh" "$REPO" "$REVISION" "$BASE_REVISION" "$BASE_JAR" \
  "$TELEMETRY_JAR" "$FLINK_LIB" "$COMPILE_LIB" "$LOMBOK_JAR" "$OUT_DIR"
python3 "$HELPER/render.py" --repo "$REPO" --namespace "$ARM_NAMESPACE" \
  --revision "$REVISION" --image "$BASE_IMAGE" --base-jar "$BASE_JAR" \
  --autoscaler "$OUT_DIR/variant.jar" --wiring "$OUT_DIR/wiring.jar" \
  --telemetry "$TELEMETRY_JAR" > "$OUT_DIR/operator.json"
```

Use `--node HOSTNAME` to pin the operator separately from the workload. The renderer checks each code JAR's recorded revision against the requested source tree. It emits the chart's namespace-scoped operator, service accounts, RBAC and configuration, plus a ConfigMap containing the small JAR overlays. Its bootstrap verifies the checksums and class origins, then enters the normal `FlinkOperator.main`. The running pod logs a `Q8_NATIVE_OPERATOR` line for each checked class.

Install into the dedicated Kubernetes context and namespace you prepared. Server-side apply avoids storing a second copy of the ConfigMap's binary JARs in annotations.

```bash
kubectl --context "$BENCH_CONTEXT" -n "$ARM_NAMESPACE" apply --server-side \
  --field-manager=q8-native-benchmark -f "$OUT_DIR/operator.json"
kubectl --context "$BENCH_CONTEXT" -n "$ARM_NAMESPACE" rollout status deployment/flink-kubernetes-operator
kubectl --context "$BENCH_CONTEXT" -n "$ARM_NAMESPACE" logs deploy/flink-kubernetes-operator | grep Q8_NATIVE_OPERATOR
sha256sum "$OUT_DIR/variant.jar"
```

Pass `--operator-deployment flink-kubernetes-operator`, `--operator-container flink-kubernetes-operator`, and `--operator-jar /opt/q8-operator/variant.jar`, together with the corresponding digest, revision and namespace, to the [Q8 runner](../QUICK.md). The Flink job and the operator run in the same namespace for this installation.
