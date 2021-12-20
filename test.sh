#!/bin/sh

cd test_cases/mapreduce_single
echo "--- Testing with a single process ---"
echo "Experimenting the WordCount application."
javac WordCount.java
java WordCount

echo "Experimenting the CountSameLengthWords application."
javac CountSameLengthWords.java
java CountSameLengthWords

echo "Experimenting the Vowels application."
javac Vowels.java
java Vowels


cd ../mapreduce_multi
echo "--- Testing with the multiple processes ---"
echo "Experimenting the WordCount application."
javac WordCount.java
java WordCount

echo "Experimenting the CountSameLengthWords application."
javac CountSameLengthWords.java
java CountSameLengthWords

echo "Experimenting the Vowels application."
javac Vowels.java
java Vowels


cd ../mapreduce_with_fault_tolerance
echo "--- Testing with the multiple processes and one fault ---"
echo "Experimenting the WordCount application."
javac WordCount.java
java WordCount

echo "Experimenting the CountSameLengthWords application."
javac CountSameLengthWords.java
java CountSameLengthWords

echo "Experimenting the Vowels application."
javac Vowels.java
java Vowels

echo "End of testing all applications. Please check application folders to check the output files.."

cd ../..
echo "--- Comparing the expected output and the output returned by the system ---"
javac CheckCorrectness.java
java CheckCorrectness
