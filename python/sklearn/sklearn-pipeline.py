import apache_beam as beam
import apache_beam.transforms.window as window

options = beam.utils.pipeline_options.PipelineOptions()

google_cloud_options = options.view_as(beam.utils.pipeline_options.GoogleCloudOptions)
google_cloud_options.project = '{YOUR_PROJECT}'
google_cloud_options.job_name = 'sklearn'
google_cloud_options.staging_location = 'gs://{YOUR_BUCKET}/binaries'
google_cloud_options.temp_location = 'gs://{YOUR_BUCKET}/temp'

worker_options = options.view_as(beam.utils.pipeline_options.WorkerOptions)
worker_options.max_num_workers = 15 #default:15

#options.view_as(beam.utils.pipeline_options.StandardOptions).runner = 'DirectRunner'
options.view_as(beam.utils.pipeline_options.StandardOptions).runner = 'DataflowRunner'

query = """
    SELECT year,date,shop_id,sales,attr1,attr2,attr3
    FROM dataset.table
"""


def assign_timevalue(v):

    import apache_beam.transforms.window as window

    return window.TimestampedValue(v, v["year"])

def learn_predict(records):

    import pandas as pd
    from sklearn.ensemble import GradientBoostingRegressor

    target_attr = 'sales'
    learn_attrs = ['attr1', 'attr2', 'attr3']

    vs = [v for v in records[1]]

    data = pd.DataFrame(vs)
    data[learn_attrs] = data[learn_attrs].apply(pd.to_numeric)
    data = data.fillna(0.)

    if len(data["year"].unique()) < 3:
        return []

    year_max = data["year"].max()
    train = data[data["year"] <  year_max]
    test  = data[data["year"] == year_max]

    model = GradientBoostingClassifier(n_estimators=3000, learning_rate=0.01, max_depth=4, max_features=0.5)
    model.fit(train[learn_attrs], train[target_attr])
    test.loc[:, "predict"] = model.predict(test[learn_attrs])

    return test[["shop_id","date","predict","sales"]].to_dict(orient='records')


pipeline = beam.Pipeline(options=options)

(pipeline
 | "Query data"  >> beam.Read(beam.io.BigQuerySource(query=query))
 | "Assign time" >> beam.Map(assign_timevalue)
 | "Set window"  >> beam.WindowInto(window.SlidingWindows(size=3, period=1))
 | "Assign group key" >> beam.Map(lambda v: ('key', v))
 | "Group by group key and time window" >> beam.GroupByKey()
 | "Learn and predict" >> beam.FlatMap(learn_predict)
 | "Write predict data"  >> beam.Write(beam.io.BigQuerySink('dataset.table',
                              schema="shop_id:STRING, date:STRING, predict:FLOAT, sales:INTEGER",
                              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)))

pipeline.run()
