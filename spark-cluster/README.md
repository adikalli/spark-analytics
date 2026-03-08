# Running Spark via Docker (Cluster mode).

We will be  submitting spark job to cluster with two worker nodes.

```bash
cd spark-cluster/
docker compose up -d --build 
docker exec -it spark-master bash
```