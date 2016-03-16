program = KitchenUse
inputFilepath = /datasets/
inputFile = household_power_consumption.txt
outputFilepath = ./test/l8p8output
jarFilepath = ./../jars/*
interPath = ./test/l8p8inter

default:
	javac -cp "$(jarFilepath)" *.java
jar:
	jar cvf $(program).jar *.class
job:
	hadoop jar $(program).jar $(program) $(inputFilepath)$(inputFile) $(outputFilepath)
remove:
	hadoop fs -rm -r $(outputFilepath) $(interPath)
