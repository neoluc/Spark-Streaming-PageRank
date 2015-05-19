from pyspark import SparkContext
from pyspark.streaming import StreamingContext

checkpoint_path = "file:///home/michael/pagerank/checkpoint"

def transform_to_url_tuple(string):
	urls = string.split()
	return (urls[0], urls[1])

def combine_list(new_list, old_list):
	re = []
	for iterable in new_list:
		for item in iterable:
			re.append(item)
	if old_list is None:
		return re
	s = set(re)
	for iterable in old_list:
		for item in iterable:
			s.add(item)
	return list(s)

def print_ranks(rdd):
	for (link, rank) in rdd.collect():
		print "%s has rank: %s" % (link, rank)
	print "-----------------------------------"

if __name__ == "__main__":
	# prepare
	sc = SparkContext("local[2]", "pagerank stream") # if is not >= 2, the program will not run
	ssc = StreamingContext(sc, 90)
	ssc.checkpoint(checkpoint_path)

	# parse all node into rdd, (1, [2, 3, 4])  (2, [3, 4]) ...
	lines = ssc.socketTextStream("localhost", 9999)
	links = lines\
		.map(transform_to_url_tuple)\
		.groupByKey()\
		.updateStateByKey(combine_list)

	# initialize all nodes to 1.0, (1, 1.0)  (2, 1.0) ...
	ranks = links.map(lambda (name, neighbours): (name, 1.0))

	# start calculate pagerank
	for i in range(10):
		# after join (1, ([2, 3, 4], 1.0))  (2, ([3, 4], 1.0)) ...
		# for 1st element, it would become (2, 1.0 / no of neighbours) (3, 1.0 / no of neighbours) (4, 1.0 / no of neighbours)
		# for 2nd element, it would become (3, 1.0 / no of neighbours) (4, 1.0 / no of neighbours)
		# then sum all value for each node
		# after flatMap (2, 0.234)  (3, 0.345)  (4, 0.456) ...
		contribs = links\
			.join(ranks)\
			.flatMap(lambda (name, (neighbours, score)): map(lambda neighbour: (neighbour, score / len(neighbours)), neighbours))
			
		# sum contribs back to rank scores
		ranks = contribs\
			.reduceByKey(lambda score_by_a, score_by_b: score_by_a + score_by_b)\
			.mapValues(lambda score: score * 0.85 + 0.15)
	
	ranks.foreachRDD(print_ranks)

	ssc.start()
	ssc.awaitTermination()