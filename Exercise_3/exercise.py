import apache_beam as beam

class SplitRow(beam.DoFn):

    def process(self,element):
        return [element.split(',')]

class FilterAccountsEmployee(beam.DoFn):

    def process(self,element):
        if element[3] == 'Accounts':
            return [element]

class PairEmployee(beam.DoFn):

    def process(self,element):
        return [ (element[3] + ',' +element[1], 1)]

class Counting(beam.DoFn):

    def process(self,element):
        (key,values) = element
        return [(key,sum(values))]

with beam.Pipeline() as p1:

  attendance_count = (
    p1
		|'Read from file' >> beam.io.ReadFromText('dept_data.txt')

		|beam.ParDo(SplitRow()) #by default returns multiple outputs
        #|beam.ParDo(lambda element: [element.split(',')])

		|beam.ParDo(FilterAccountsEmployee())

		|beam.ParDo(PairEmployee())

        |'Group' >> beam.GroupByKey()
        |'Sum using ParDo' >> beam.ParDo(Counting())

		|beam.io.WriteToText('Exercise_3/output_new')
    )
