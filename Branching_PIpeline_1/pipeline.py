import apache_beam as beam

p = beam.Pipeline()


input_collection = (
    p
    | 'Read from text file' >> beam.io.ReadFromText('dept_data.txt')
    | 'Split rows' >> beam.Map(lambda record: record.split(','))
)

accounts_count = (
        input_collection
		|'Get all Accounts dept persons' >> beam.Filter(lambda record: record[3]== 'Accounts')

		|'Pair each account employee with 1' >> beam.Map(lambda record: (record[1],1))
		|'Group and sum 1' >> beam.CombinePerKey(sum)

		#|'Write results for account' >> beam.io.WriteToText('Branching_Pipeline_1/Accounts')
    )

hr_count = (
        input_collection
		|'Get all HR dept persons' >> beam.Filter(lambda record: record[3]== 'HR')

		|'Pair each hr employee with 1' >> beam.Map(lambda record: (record[1],1))
		|'Group and sum1' >> beam.CombinePerKey(sum)

		#|'Write results for HR' >> beam.io.WriteToText('Branching_Pipeline_1/HR')
    )

output = (
    (accounts_count,hr_count)
    |beam.Flatten() #they need to have the same number of columns and data types
    |beam.io.WriteToText('Branching_Pipeline_1/Both')


)
p.run()
