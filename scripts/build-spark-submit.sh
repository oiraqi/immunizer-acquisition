cd ../framework
./gradlew libs
cd ../../spark-2.4.5-bin-hadoop2.7/bin
./spark-submit --class org.immunizer.monitor.MonitorApplication --master spark://spark-master:7077 --deploy-mode cluster /data/libs-1.0.jar