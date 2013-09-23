JFLAGS = -g
JC = javac
TESTFILE = RCV1.very_small_test.txt
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
        NBTest.java \
        NBTrain.java 

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

demo:
	cat RCV1.very_small_train.txt | java NBTrain | java NBTest RCV1.very_small_test.txt

test:
	cat RCV1.very_small_train.txt | java NBTrain | java NBTest $(TESTFILE)
