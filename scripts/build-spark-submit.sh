cd ../framework
./gradlew libs
cp build/libs/libs-1.0.jar /root/data/
cd ../../spark-2.4.5-bin-hadoop2.7/bin
./spark-submit --class org.immunizer.monitor.MonitorApplication --master spark://spark-master:7077 --deploy-mode cluster --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true /data/libs-1.0.jar --jars /data/libs-1.0.jar
