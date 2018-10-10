import lp_config as cfg
import datetime as dt
import argparse
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import lp_helper as lph
from pyspark.sql.functions import explode, col
import argparse
import json



class create_lp_image(object):
    
    def __init__(self,dts,game):
        conf = SparkConf().setAppName("Creating leanplum base data from logs")
        conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        self.sc = SparkContext(conf=conf)
        self.sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", cfg.access_key)
        self.sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", cfg.secret_key)
        self.sqlcontext = SQLContext(sc)
        self.sqlcontext.udf.register("change_to_normal_date", lph.change_to_normal_date)
        self.dt = dts
        self.game = game
        assert game > ' ','Games cannot be null.Choices are ce or l2dl'
        
        
    def read_lp_log_as_df(self,incoming_path):
        try:
            self.lp_sessions = self.sqlcontext.read.json(incoming_path)
            return self.lp_sessions.count()
        except Exception as e:
            print('Could not create the base file due to {}'.format(e))
            return 0 
    
    def initiate_the_header_data(self):
        self.create_sessions_with_states()
        self.create_sessions_with_no_states()
        adf = self.flatten_session_with_states()
        self.create_states_with_no_events(adf)
        self.create_states_with_events(adf)
        self.flatten_states_with_events()
        return self.create_header_data()
        
    
    def create_sessions_with_no_states(self):
        self.sessions_with_no_states = self.lp_sessions.where(self.lp_sessions.states.isNull())
        
    
        
    def create_sessions_with_states(self):
        self.sessions_with_states = self.lp_sessions.where(self.lp_sessions.states.isNotNull())
        
    def flatten_session_with_states(self):
        #self.session_with_states_flattened
        return self.sessions_with_states.select('sessionId','deviceId','city','client','country','lat','lon','priorEvents','priorSessions','priorTimeSpentInApp','region','sdkVersion','deviceModel','duration','firstRun','isDeveloper','isSession','locale','systemName','systemVersion','time','timezone','userId','userBucket','userAttributes',explode("states").alias('states'))
        
    def create_states_with_no_events(self,adf):
        self.states_with_no_events = adf.where(adf['states.events'].isNull())

        
    def create_states_with_events(self,adf):
        self.states_with_events = adf.where(adf['states.events'].isNotNull())
        
                
        
    def flatten_states_with_events(self):
        try:
            self.stateswithevents_flattened = self.states_with_events.select('sessionId','deviceId','city','client','country','lat','lon','priorEvents','priorSessions','priorTimeSpentInApp','region','sdkVersion','deviceModel','duration','firstRun','isDeveloper','isSession','locale','systemName','systemVersion','time','timezone','userId','userBucket',col('userAttributes.adid').alias('adid'),col('states.stateid').alias('stateid'),col('states.events.name').alias('statename'),explode("states.events").alias('events'))
        except Exception as e:
            print('Could not create the states with events due to {}'.format(e))
            return self.sqlcontext.createDataFrame(self.sc.emptyRDD(), cfg.schema)
        
    def create_header_data(self):
        try:
            _ = self.sessions_with_no_states.registerTempTable("sessions_with_no_states")
            _ = self.states_with_no_events.registerTempTable("states_with_no_events")
            _ = self.stateswithevents_flattened.registerTempTable("states_with_events")

            return self.sqlcontext.sql(cfg.header_sql)
        except Exception as e:
            print('Could not create the header file due to {}'.format(e))
            return self.sqlcontext.createDataFrame(self.sc.emptyRDD(), cfg.schema)
        
        
    def flush_df_data_into_files(self,df,typeof):
        try:
            assert typeof > ' ', 'Must indicate, what type of out put it is. Headers/events or states'
            avaliddt = (dt.datetime.strptime(self.dt, '%Y-%m-%d')).strftime('%Y%m%d')
            path_to_put_the_file = 's3a://' + cfg.lp_bucket + '/games/' + self.game + '/' + typeof + '/plogdate=' + avaliddt
            df.write.parquet(path_to_put_the_file,mode="overwrite")
            return True
        except Exception as e:
            print('Could not write the File due to {}'.format(e))
            return False
         
    def create_df_for_states_with_no_events(self):
        try:
            return self.sqlcontext.sql(cfg.states_with_no_events)
        except Exception as e:
            print('Could not create the states with no events due to {}'.format(e))
            return self.sqlcontext.createDataFrame(self.sc.emptyRDD(), cfg.schema)
        
    def create_df_with_event_details(self):
        try:
            states_with_events_flattened=self.stateswithevents_flattened.select('userid','sessionid','stateid' ,col('events.value').alias('value'),col('events.parameters.traceid').alias('traceid'),col('events.parameters.eventCategory').alias('eventcategory'),col('events.parameters').alias('parameters'),col('events.time').alias('utctime'),col('events.eventid').alias('eventid'),col('events.name').alias('name'))
            results = states_with_events_flattened.toJSON().map(lambda j: json.loads(j))
            atempset = results.map(lambda x: lph.change_parameter_2_string(x))
            return atempset.toDF()
        except Exception as e:
            print('Could not create the event detail dataframe due to {}'.format(e))
            return self.sqlcontext.createDataFrame(self.sc.emptyRDD(), cfg.schema)

    def execute(self,method,*args,**kargs):
        method_maps={
           'header':self.initiate_the_header_data,
           'states':self.create_df_for_states_with_no_events,
           'events':self.create_df_with_event_details,
           'flush': self.flush_df_data_into_files
            }
            return method_maps[method](*args,**kargs)

def main():
    parser = argparse.ArgumentParser(description='Get parameters for Leanplum Base table calc process.')
    parser.add_argument("startDt", help="Indicates the starting Date",type=valid_date)
    parser.add_argument("endDt", help="Indicates the ending Date",type=valid_date)
    parser.add_argument("games", help="Indicates the game for which LP logs are derived",choices=['ce', 'l2dl'],default='ce')
    
    startDt,endDt = args.startDt, args.endDt
    while startDt <= endDt:
        get_lp_image = create_lp_image(startDt,args.games)
        incoming_path=cfg.lp_source[args.game] + 'dt=' + lph.get_date_string(startDt) + '/'
        if get_lp_image.read_lp_log_as_df(incoming_path) > 0:
            for steps in cfg.method_steps:
                resp_df = get_lp_image.execute(steps)
                if resp_df.count() > 0:
                    if not get_lp_image.execute('flush',resp_df,steps):
                        print('Could not create the files on s3')
            print('Successfully created dataframe for the given dates {}'.format(startDt))

        startDt = (datetime.strptime(startDt,'%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')



if __name__ == "__main__":
    main()