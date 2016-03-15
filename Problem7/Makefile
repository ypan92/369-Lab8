program = PowerDays
inputFilepath = /datasets/
inputFile = household_power_consumption.txt
outputFilepath = ./test/l8p7output
jarFilepath = ./../jars/*
mapReduceJarFilepath = ./../jars/json-mapreduce-1.0.jar
jsonJarFilepath = ./../jars/org.json-20120521.jar
interPath = ./test/l8p7inter

default:
	javac -cp "$(jarFilepath)" *.java
jar:
	jar cvf $(program).jar *.class
job:
	hadoop jar $(program).jar $(program) -libjars $(mapReduceJarFilepath),$(jsonJarFilepath) $(inputFilepath)$(inputFile) $(outputFilepath)
remove:
	hadoop fs -rm -r $(outputFilepath) $(interPath)