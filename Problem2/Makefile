program = LargestValue
inputFilePath1 = /datasets/poker-hand-traning.true.data.txt
inputFilePath2 = /datasets/poker-hand-testing.data.txt
outputFilePath1 = ./highestValueRaw
outputFilePath2 = ./formattedHighestValue
jarFilepath = ./../jars/*

default:
	javac -cp "$(jarFilepath)" $(program).java
jar:
	jar cvf $(program).jar *.class
job:
	hadoop jar $(program).jar $(program) $(inputFilePath1) $(inputFilePath2) $(outputFilePath1) $(outputFilePath2)
remove:
	hadoop fs -rm -r $(outputFilePath1) $(outputFilePath2)