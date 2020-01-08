cd ../framework
./gradlew libs
cd build/libs
java -cp libs-1.0.jar org.immunizer.acquisition.AcquisitionApplication
