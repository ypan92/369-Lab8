program = SlashdotReach
inputFilePath = /datasets/soc-Slashdot0811.txt
outputFilePath1 = ./highestValueRaw
outputFilePath2 = ./formattedHighestValue

default:
	javac -cp hadoop-core-1.2.1.jar $(program).java
jar:
	jar cvf $(program).jar *.class
job:
	hadoop jar $(program).jar $(program) $(inputFilePath) $(outputFilePath1) $(outputFilePath2)
remove:
	hadoop fs -rm -r $(outputFilePath1) $(outputFilePath2)