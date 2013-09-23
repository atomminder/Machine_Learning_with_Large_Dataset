JFLAGS = -g
JC = javac
TESTFILE = abstract.tiny.test
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
        UniCounter.java \
        BiCounter.java \
		Message.java \
        Answer.java \
        Combine.java \
		Rank.java \
        UniGlobalCount.java \
        BiGlobalCount.java
        
default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
	$(RM) *.dat

demo:
	cat unigram_apple.txt  | sort  | java -Xmx128m UniCounter > Uni_Counters.dat 
	cat bigram_apple.txt | sort | java -Xmx128m BiCounter > Bi_Counters.dat
	java -Xmx128m UniGlobalCount Uni_Counters.dat  > Uni_Globals.dat
	java -Xmx128m BiGlobalCount Bi_Counters.dat > Bi_Globals.dat
	cat Bi_Counters.dat | java -Xmx128m Message > messages.dat
	cat messages.dat Uni_Counters.dat | sort -k1 | java -Xmx128m Answer  > step4_results.dat
	cat step4_results.dat Bi_Counters.dat | sort | java -Xmx128m Combine > step5_results.dat
	cat Uni_Globals.dat Bi_Globals.dat step5_results.dat | java -Xmx128m Rank

test:
	cat unigram_full.txt  | sort | java -Xmx128m UniCounter > Uni_Counters.dat 
	cat bigram_full.txt | sort |java -Xmx128m BiCounter > Bi_Counters.dat
	java -Xmx128m UniGlobalCount Uni_Counters.dat  > Uni_Globals.dat
	java -Xmx128m BiGlobalCount Bi_Counters.dat > Bi_Globals.dat
	cat Bi_Counters.dat | java -Xmx128m Message > messages.dat
	cat messages.dat Uni_Counters.dat | sort -k1  | java -Xmx128m Answer > step4_results.dat
	cat step4_results.dat Bi_Counters.dat | sort | java -Xmx128m Combine > step5_results.dat
	cat Uni_Globals.dat Bi_Globals.dat step5_results.dat | java -Xmx128m Rank
