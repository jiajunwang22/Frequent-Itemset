# Frequent-Itemset
part of Data Mining HW(PCY/SON/Apriori)

1. PCY

Implemented the PCY algorithm. Recall that the algorithm leverages the unused space in the first phase
to build a frequent-bucket hash table which is then used in the second phase to reduce the number of
candidate itemset to be considered.

The algorithm needs to output all the discovered frequent items (Singletons and pairs) in
frequentset.txt and all the candidate itemset the hash table helps prune in the second phase of the
algorithm in candidates.txt in the directory <output-dir>. In addition, it needs to output the false
positive rate of the hash table (3 decimal places) on the console as below.
  
2. SON&Apriori

Implemented SON algorithm “FirstName_LastName_SON.py” in Apache Spark using Python. 

Recall that given a set of baskets, SON algorithm divides them into chunks/partitions and then proceed in two stages. First, local frequent itemsets are collected, which form candidates; next, it makes second pass through data to determine which candidates are globally frequent.

Implemented Apriori algorithm for stage one to find local frequent itemset. Make use of monotonicity concept while generating candidate itemset.

Saved all the frequent itemset into one file. Each line will have frequent items in sorted
ascending order. Singletons followed by pairs, triples, quadruples etc. Frequent items except singletons
need to be in tuple format, and are sorted in ascending order within themselves.
