# DynamicArrayBenchmark

1. go to project folder.
2. "mvn clean install"
3. go to target folder and then
  for throughput : "java -jar benchmarks.jar -wi 5 -i 10 -f 2 -t 1"
  for average time per operation : "java -jar benchmarks.jar -wi 5 -i 10 -f 2 -t 1 -bm avgt -tu ms"
