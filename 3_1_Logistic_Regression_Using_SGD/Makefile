JFLAGS = -g
JC = javac
TESTFILE = abstract.tiny.test
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
        sgdLR.java \
        globalParameters.java 

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class

demo:
	java sgdLR abstract.tiny.train abstract.tiny.test

test:
	java sgdLR abstract.tiny.train $(TESTFILE)
