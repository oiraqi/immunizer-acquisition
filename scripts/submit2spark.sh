cd ../../spark-2.4.5-bin-hadoop2.7/bin
./spark-submit --class org.immunizer.microservices.monitor.MonitorApplication --master spark://spark-master:7077 --deploy-mode cluster --jars /libs/lib.jar /libs/framework-1.0.jar
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    