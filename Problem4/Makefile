program = SeedNormKNN
inputFilePath = /datasets/seeds_dataset.txt
outputFilePath = ./seedNormOutput

default:
	javac -cp hadoop-core-1.2.1.jar $(program).java
jar:
	jar cvf $(program).jar *.class
job:
	hadoop jar $(program).jar $(program) $(inputFilePath) $(outputFilePath)
remove:
	hadoop fs -rm -r $(outputFilePath)