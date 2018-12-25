# h(i,j) = (a*i + b*j) % N
import sys
import os

a = int(sys.argv[2])
b = int(sys.argv[3])
N = int(sys.argv[4])
#s is threshold
s = int(sys.argv[5])

counttable = {}
frequentitem = []
lines = []
candidates = {}
frequentbuckets = {}
bitmap = []
frequentitems = []
pruncandidates = []
pass2count = {}
falsepositive = 0

file_path = "./" + sys.argv[6] + "/"
directory = os.path.dirname(file_path)

try:
    os.stat(directory)
except:
    os.mkdir(directory)


for i in range(0,N):
    frequentbuckets[i] = 0

with open(sys.argv[1],"r") as file:
    lines = file.readlines()

for line in lines:
    numbers = line.strip().split(",")
    # count table
    for n in numbers:
        if n not in counttable:
            counttable[n] = 1
        else:
            counttable[n] += 1


    #frequent buckets
    for i in range(0, len(numbers) - 1):
        for j in range(i + 1, len(numbers)):
            frequentbuckets[(a * int(numbers[i]) + b * int(numbers[j])) % N] += 1

# frequent item
for num in counttable:
    if counttable[num] >= s:
        frequentitem.append(num)


frequentitem.sort(key=int)

# #frequent buckets bitmap
for bucket in frequentbuckets:
    if frequentbuckets[bucket] >= s:
        bitmap.append(bucket)

falsepositive = float(len(bitmap)) / len(frequentbuckets)
print "False Postitive Rate: %.3f" % falsepositive

# initial candidates
for i in range(0, len(frequentitem) - 1):
    for j in range(i + 1, len(frequentitem)):
        if (a * int(frequentitem[i]) + b * int(frequentitem[j])) % N not in bitmap:
            pruncandidates.append((int(frequentitem[i]),int(frequentitem[j])))
        else:
            if (int(frequentitem[i]),int(frequentitem[j])) not in candidates:
                candidates[(int(frequentitem[i]),int(frequentitem[j]))] = 0

for line in lines:
    numbers = line.strip().split(",")

    # count candidates
    for i in range(0, len(numbers) - 1):
        for j in range(i + 1, len(numbers)):
            if (int(numbers[i]),int(numbers[j])) in candidates:
                candidates[(int(numbers[i]),int(numbers[j]))] += 1

for candidate in candidates:
    if candidates[candidate] >= s:
        frequentitems.append(candidate)

frequentitems.sort()

frequentitem = frequentitem + frequentitems


with open(file_path + "candidates.txt","w") as wc:
    for prun in pruncandidates:
        wc.write(str(prun).replace(" ","") + "\n")

with open(file_path + "frequentset.txt", "w") as wf:
    for value in frequentitem:
        wf.write(str(value).replace(" ","") + "\n")


