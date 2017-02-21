import apache_beam as beam
import apache_beam.transforms.window as window

if __name__ == '__main__':

    options = beam.utils.pipeline_options.PipelineOptions()

    google_cloud_options = options.view_as(beam.utils.pipeline_options.GoogleCloudOptions)
    google_cloud_options.project = '{YOUR_PROJECT}'
    google_cloud_options.job_name = 'xgboost'
    google_cloud_options.staging_location = 'gs://{YOUR_BUCKET}/binaries'
    google_cloud_options.temp_location = 'gs://{YOUR_BUCKET}/temp'

    worker_options = options.view_as(beam.utils.pipeline_options.WorkerOptions)
    worker_options.max_num_workers = 15

    setup_options = options.view_as(beam.utils.pipeline_options.SetupOptions)
    #setup_options.requirements_file = "requirements.txt"
    #setup_options.extra_packages = ["/home/orfeon/local/someapplication/dist/someapplication-0.0.1.tar.gz"]
    setup_options.setup_file = "/home/orfeon/dataflow-sample/python/xgboost/setup.py" # set fullpath

    #options.view_as(beam.utils.pipeline_options.StandardOptions).runner = 'DirectRunner'
    options.view_as(beam.utils.pipeline_options.StandardOptions).runner = 'DataflowRunner'

    pipeline = beam.Pipeline(options=options)

    query = """
        SELECT year,date,shop_id,sales,attr1,attr2,attr3
        FROM dataset.table
    """


    def assign_timevalue(v):

        import apache_beam.transforms.window as window
        return window.TimestampedValue(v, v["year"])

    def learn_predict(records):

        import pandas as pd
        from sklearn.metrics import log_loss
        import xgboost as xgb

        data = pd.DataFrame(records[1])
        target_attr = 'sales'
        learn_attrs = ['attr1', 'attr2', 'attr3']
        data[learn_attrs] = data[learn_attrs].apply(pd.to_numeric)
        data = data.fillna(0.)
        print(data["year"].unique())

        if len(data["year"].unique()) < 3:
            return []

        year_max = data["year"].max()
        train = data[data["year"] <  year_max]
        test  = data[data["year"] == year_max]

        dtrain = xgb.DMatrix(train[learn_attrs].values, label=train[target_attr].values, feature_names=learn_attrs)
        dtest  = xgb.DMatrix(test[learn_attrs].values, label=test[target_attr].values, feature_names=learn_attrs)

        evals_result = {}
        watchlist = [(dtrain, 'train'),(dtest, 'test')]
        best_params = {'objective': 'reg:linear',
                       'eval_metric': 'rmse',
                       'learning_rate': 0.01,
                       'max_depth': 3,
                       'colsample_bytree': 0.65,
                       'subsample': 0.55,
                       'min_child_weight': 7.0,
                       'reg_alpha': 0.6,'reg_lambda': 0.7,'gamma': 1.2}

        model = xgb.train(best_params, dtrain,
                          num_boost_round=1000,
                          evals=watchlist,
                          evals_result=evals_result,
                          verbose_eval=True)

        test.loc[:, "predict"] = model.predict(dtest)

        return test[["shop_id","date","predict","sales"]].to_dict(orient='records')


    (pipeline
     | "Query data"  >> beam.Read(beam.io.BigQuerySource(query=query))
     | "Assign time" >> beam.Map(assign_timevalue)
     | "Set window"  >> beam.WindowInto(window.SlidingWindows(size=3, period=1))
     | "Set group key"  >> beam.Map(lambda v: ('shop_id', v))
     | beam.GroupByKey()
     | "Learn and predict" >> beam.FlatMap(learn_predict)
     | "Write data" >> beam.Write(beam.io.BigQuerySink('dataset.table',
                                  schema="shop_id:STRING, date:STRING, predict:FLOAT, sales:INTEGER",
                                  write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)))

    pipeline.run()
