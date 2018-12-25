# -*- coding: UTF-8 -*-

from __future__ import print_function
import itertools as it
from pyspark import SparkContext
from operator import add
import sys

def findlocalfrequent(lines):
    baskets = [[item for item in line.strip().split(",")] for line in lines]

    threshold = support * len(baskets)


    # singlton apriori
    singltons = {}
    frequent = []
    for values in baskets:
        # singlton
        for v in values:
            if v not in singltons:
                singltons[v] = 1
            else:
                singltons[v] += 1

    singltonfrequent = sorted([v for v in singltons if singltons[v] >= threshold], key=unicode)
    frequent = singltonfrequent
    previousfrequent = singltonfrequent


    k = 1
    # apriori
    while True:
        if len(previousfrequent) < k:
            break

        k += 1
        # k-frequent
        currfrequent = []
        # all digit of k - 1 frequent
        alldigit = []
        if k == 2:
            alldigit = singltonfrequent
        else:
            for item in previousfrequent:
                alldigit += list(item)

        alldigit = sorted(list(set(alldigit)), key=unicode)

        candidates = set()

        if k == 2:
            for item in it.combinations(alldigit, k):
                number = 0
                for i in singltons:
                    if i in previousfrequent:
                        number += 1
                if number >= k:
                    candidates.add(item)
        else:
            for item in it.combinations(alldigit, k):
                number = 0
                for i in list(it.combinations(sorted(list(item)), k - 1)):
                    if i in previousfrequent:
                        number += 1
                if number >= k:
                    candidates.add(item)

        # for count frequent items
        countcandidates = {}
        for candidate in candidates:
            countcandidates[candidate] = 0

        for values in baskets:
            for candidate in countcandidates:
                if candidate in values:
                    candidates[candidate] += 1
                else:
                    temp = list(candidate)
                    if set(temp).issubset(set(values)):
                        countcandidates[candidate] += 1

        for candidate in countcandidates:
            if countcandidates[candidate] >= threshold:
                currfrequent.append(candidate)

        previousfrequent = [x for x in currfrequent]
        frequent = frequent + previousfrequent

    return frequent


def countfrequentitems(lines):
    count = {}
    for i in l:
        count[i] = 0

    for basket in lines:
        values = basket.strip().split(",")
        for c in count:
            if c in values:
                count[c] += 1
            else:
                digits = list(c)
                if set(digits).issubset(set(values)):
                    count[c] += 1
    return count.items()


def tuple_compare(x,y):
    if type(x) == unicode:
        if type(y) == unicode:
            if int(x) > int(y):
                return -1
            if int(x) < int(y):
                return 1
            return 0
        else:
            return 1
    if type(y) == unicode:
        return -1
    if len(x) > len(y):
        return -1
    if len(x) < len(y):
        return 1
    for i in range(0,len(x)):
        if int(x[i]) > int(y[i]):
            return -1
        if int(x[i]) < int(y[i]):
            return 1
    return 0


if __name__ == "__main__":

    support = float(sys.argv[2])
    sc = SparkContext(appName="FreqItemsetSON")
    lines = sc.textFile(sys.argv[1])
    size = len(lines.collect())

    threshold = support * size
    phase1 = sc.union([lines.mapPartitions(findlocalfrequent)]).distinct().collect()
 
    l = phase1

    phase2 = lines.mapPartitions(countfrequentitems).reduceByKey(add).filter(lambda x: x[1] >= threshold).map(lambda x:x[0]).collect()

    sortl2 = []

    for tup in phase2:
        if type(tup) != unicode:
            ll = []
            for number in tup:
                ll.append(number)
            sortl2.append(sorted(ll,key=int))
        else:
            sortl2.append(tup)

    sortl2.sort(cmp=tuple_compare,reverse=True)
    sc.stop()

    with open(sys.argv[3], "w") as w:
        for item in sortl2:
            if type(item) == unicode:
                w.write(str(item)+"\n")
            else:
                w.write("(" + ",".join(i for i in item) + ")\n" )




