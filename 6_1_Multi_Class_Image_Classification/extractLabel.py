inFile = open("result_0.00008.txt")
outFile = open('val_balanced.txt', 'w')
count = 0
while 1:
    line = inFile.readline()
    count = count + 1
    if(count <= 400):
        continue
    if(cmp(line[0],"P") == 0):
        break
    if not line:
        break
    pos1 = line.index(":")
    pos2 = line[pos1+1:].index(":")
    words = line[pos1+pos2+1+1:].split("\t")
    print words
    outFile.write(words[0])
    outFile.write(words[1])

inFile.close()
outFile.close()