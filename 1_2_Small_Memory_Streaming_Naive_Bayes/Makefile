JFLAGS = -g
JC = javac
TESTFILE = abstract.tiny.test
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
        NBTest.java \
        NBTrain.java \
	MyCountAdder.java 

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

demo:
	cat abstract.tiny.train | java -Xmx128m NBTrain | sort -k1,1 | java -Xmx128m MyCountAdder | sort -k1,1 | java NBTest -t abstract.tiny.test

test:
	cat abstract.tiny.train | java -Xmx128m NBTrain | sort -k1,1 | java -Xmx128m MyCountAdder | sort -k1,1 | java NBTest -t  $(TESTFILE)
