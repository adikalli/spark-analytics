# Running Spark via Docker (Cluster mode).

We will be  submitting spark job to cluster with two worker nodes.

```bash
cd spark-cluster/
docker compose up -d --build 
docker exec -it spark-master bash
```

Submit Job.<br>

```bash
/opt/spark/bin/spark-submit  \
--master spark://spark-master:7077 \
--executor-memory 512m \
--total-executor-cores 6 \
jobs/tpch_revenue_by_region_year.py
```

Stop the cluster after use.

```bash
docker compose down 
```

