cd ../framework
./gradlew build
cp build/libs/framework-1.0.jar /root/data/
cd ../../spark-2.4.5-bin-hadoop2.7/bin
./spark-submit --class org.immunizer.monitor.MonitorApplication --master spark://spark-master:7077 --deploy-mode cluster /data/framework-1.0.jar --jars /data/framework-1.0.jar
