from mrjob.job import MRJob
from mrjob.step import MRStep

class aplicacionmapreduce(MRJob):
	def steps(self):
		return [
			MRStep(mapper=self.mapper_get_score,
				reducer=self.reducer_count_score)
		]
	def mapper_get_score(self, _, line):
		(review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp) = line.split(",")
		yield review_score, 1
		
	def reducer_count_score(self, key, values):
		yield key, sum(values)

if __name__=="__main__":
	aplicacionmapreduce.run()
	
	