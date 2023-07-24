import apache_beam as beam

def retTuple(element):
    thisTuple = element.split(',')
    return (thisTuple[0],thisTuple[1:])

p = beam.Pipeline()


dep_rows = (
    p
    | 'Read from dept_data file' >> beam.io.ReadFromText('dept_data.txt')
    | 'Pair each employee with key' >> beam.Map(retTuple) #{149633CM: [Marco,10,Accounts,1-01-2019]}

)

loc_rows = (
    p
    | 'Read from location file' >> beam.io.ReadFromText('location.txt')
    | 'Pair each loc with key' >> beam.Map(retTuple)

)

results = ({'dep_data': dep_rows, 'loc_data':loc_rows}

           |beam.CoGroupByKey()
           |'Write results' >> beam.io.WriteToText('Exercise_6/output')
           )


p.run()
