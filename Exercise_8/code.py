import apache_beam as beam


class ProcessWords(beam.DoFn):
    def process(self,element,cutoff_length,marker):

        name = element.split(',')[1]

        if name.startswith(marker):
            return [name]

        if len(name) <= cutoff_length:
            return [beam.pvalue.TaggedOutput('Short_Names',name)]
        elif len(name) > cutoff_length:
            return [beam.pvalue.TaggedOutput('Long_Names',name)]




p = beam.Pipeline()

results = (
           p
        |beam.io.ReadFromText('dept_data.txt')

        |beam.ParDo(ProcessWords(),cutoff_length=4,marker='A').with_outputs('Short_Names','Long_Names',main='Names_A')

)

short_collection = results.Short_Names
long_collection = results.Long_Names
startA_collection = results.Names_A

short_collection | 'Write 1' >> beam.io.WriteToText('Exercise_8/short')
long_collection | 'Write 2' >> beam.io.WriteToText('Exercise_8/long')
startA_collection | 'Write 3' >> beam.io.WriteToText('Exercise_8/start_a')

p.run()
