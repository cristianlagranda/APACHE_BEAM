import apache_beam as beam

class Average(beam.CombineFn):
    def create_accumulator(self):
       return (0.0,0) #initialize (sum,count)

    def add_input(self, sum_count,input):
       (sum,count) = sum_count
       return sum + input, count + 1

    def merge_accumulators (self,accumulators):
        ind_sums,ind_counts = zip(*accumulators) #zip - [(27,3),(39,3),(18,2)] -> [(27,39,18),(3,3,2)]
        return sum(ind_sums),sum(ind_counts) # (84,8)

    def extract_output(self, sum_count):
       (sum,count) = sum_count
       return sum/count if count else float('NaN')

with beam.Pipeline() as p1:

  small_sum = (
    p1
		|beam.Create([15,5,7,7,9,23,13,5])

        |beam.CombineGlobally(Average())


		|beam.io.WriteToText('Exercise_4/output_new')
    )
