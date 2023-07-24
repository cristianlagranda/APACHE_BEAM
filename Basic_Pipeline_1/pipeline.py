import apache_beam as beam

with beam.Pipeline() as p1:

  attendance_count = (
    p1
		|'Read from file' >> beam.io.ReadFromText('dept_data.txt')

		|beam.Map(lambda record: record.split(',')) # one input, one output. En este ejemplo devuelve una lista con los 5 elementos

		|beam.Filter(lambda record: record[3]== 'Accounts')

		|beam.Map(lambda record: (record[1],1))
		|beam.CombinePerKey(sum)

		|beam.io.WriteToText('Basic_Pipeline_1/output_new')
    )
