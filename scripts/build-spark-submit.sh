cd ../framework
./gradlew libs
cd ../../spark-2.4.5-bin-hadoop2.7/bin
./spark-submit --class org.immunizer.monitor.MonitorApplication --master local[8] /root/data/libs-1.0.jar
