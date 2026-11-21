# Build the Q8 benchmark artifacts

Use the checked-out `a4s-cache-miss-penalty` source on Linux amd64. The lab used JDK 17 with Java 11 class targets. The native build requires Docker and the pinned FRocksDB submodule. The lab used its local Maven cache, including Flink 1.18.1, Beam Nexmark 2.3.0 and Maven Shade 3.6.2. The script uses Maven offline. On a new machine, build the Q8 Maven module once with network access to populate that cache.

```bash
cd "$REPO"
git submodule update --init sources/libs/frocksdb
bash images/frocksdb/build.sh
NATIVE_JAR="$REPO/sources/flink/flink-runtime-1-18-a4s/rocksdbjni-6.20.3-linux64.jar"
bash "$EXPERIMENT/build-lab-jars.sh" "$REPO" "$DIST_LIB" "$BUILD_DEPS" "$NATIVE_JAR" "$ARTIFACTS"
export AUTOSCALER_JAR="$ARTIFACTS/branch-autoscaler.jar"
(cd "$ARTIFACTS" && sha256sum -c sha256sums.txt)
```

Set `EXPERIMENT` to the directory containing this file. `DIST_LIB` is the lib directory from the benchmark's A4S Flink 1.18-SNAPSHOT runtime. The lab's `flink-dist-1.18-SNAPSHOT.jar` has SHA-256 `231d969d8683e98f10f4f4599750bf7ad423827f3f609ccb4df337558d9d0ae8`. The image digest is pinned in `deployment.json`. `BUILD_DEPS` is a directory of compile/test JARs. The lab used Lombok 1.18.30, jsr305 3.0.2, JetBrains annotations 13.0, SLF4J API 1.7.36, Quartz 2.4.0, HikariCP 5.1.0, AssertJ 3.26.3 and JUnit Platform Console Standalone 1.9.3. The controller process uses the distribution libraries and branch JARs. Keep the telemetry JAR before the distribution in its classpath so the worker and controller share the same curve classes.

The build compiles the replay source and checks checkpoint/restore for both event types, builds the resource-profile and telemetry overlays, and compiles the full autoscaler. It exercises the estimator, planning, metric evaluator and replacement-job profile tests. Use `README.md` to compile the private policy driver and launch the pairs with one set of artifacts.

## Reference lab binaries

| File | Source used in the lab | SHA-256 |
|---|---|---|
| `Query8.jar` | Checkpointed inputs at `01e7cf22`. Shaded Q8 module with unchanged query topology. | `0df4ad393672463ee5a549f1dddcc5f6260174c852109d210d9c917a5744de39` |
| `rocksdbjni.jar` | FRocksDB `d439c0fb` and the branch's latency and lookup patches at `0c0c818a`. The default native builder applies both patches from `32a9091a`. | `b1368b4f01710658f059a24a12d83013fd4fadedb9bba7b3956f4afeca658c51` |
| `abi3-mrc-telemetry.jar` | Runtime telemetry, histogram transport and Java curve classes at `0c0c818a`. | `868d93fe641c58f3205e20d5f76e99e10950a426ab1891bcc300f2e2a6248d4c` |
| `savepoint-profiles.jar` | Opt-in submit-time profile overrides at `0489c2e9`. Each of the four rebuilt class files is byte-identical to the corresponding class in the pinned lab JAR. | `5aba5b0877557ce377ded976429e041c79658f675ec1a71fb880227ad3da3da7` |
| `branch-autoscaler.jar` | Complete autoscaler at `0c0c818a`, including the estimator, common-intercept option and per-slot conversion. | `27f27e1ba422f1d495dcf72c3240186416e8e17607f96dbb0ce34afa46298d48` |

JAR timestamps, manifests and build tools can change archive hashes across rebuilds. Pin and compare the hashes emitted for the particular run. Both arms must use those same bytes. The profile override takes effect only with `pipeline.jobvertex-resourceprofile-overrides-on-submit.enabled=true`. It validates the entire override map before changing any slot group on the replacement job.

## Bind artifacts to a comparison plan

Use the checksummed bytes for the particular trial and the full repaired source tree from the authority handoff when preparing the [offline comparison plan](QUICK.md). Record the FRocksDB gitlink and the actual ordered patch hashes from that tree. Historical hashes above identify the earlier lab binaries.

Keep the existing in-container JAR checks and class-origin proof. The operator renderer records requested base, policy, wiring and telemetry hashes in deployment annotations. Record image identity and actual class or native-library evidence separately from the requested digests. Leave observed-loaded fields unavailable until the corresponding receipts exist.
