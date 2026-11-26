# ECE496 benchmark work

- Before deploying or comparing benchmarks, read [Experiment Flow](docs/experiment-flow.md). It covers lab access through fs.csl, workload isolation, and the shared measurement procedure.
- Start with the [Nexmark Q8 DataStream reference](docs/experiment-flow.md#reference-benchmark-nexmark-q8-datastream) for the 80k comparison. Use the selected experiment's runbook for its input, deployment, and cleanup procedure.
- Verify the Kubernetes context, namespace ownership, worker capacity, and artifact identities before deployment. Keep SSH material, kubeconfigs, and registry credentials out of the repository and evidence archives.
