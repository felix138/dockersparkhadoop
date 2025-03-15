docker exec resourcemanager chmod -R 777 /tmp/hadoop-root
docker exec datanode chmod -R 777  /hadoop/dfs/data

docker exec datanode1 chmod -R 777  /hadoop/dfs/data
docker exec datanode2 chmod -R 777  /hadoop/dfs/data