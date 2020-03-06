cd ../framework
./gradlew build
./gradlew libs
cp build/libs/framework-1.0.jar /root/data/
cp build/libs/libs-1.0.jar /root/data/
cd ../../spark-2.4.5-bin-hadoop2.7/bin
./spark-submit --class org.immunizer.monitor.MonitorApplication --master spark://spark-master:7077 --deploy-mode cluster --jars /libs/apache.jar,/libs/misc.jar /libs/framework-1.0.jar
