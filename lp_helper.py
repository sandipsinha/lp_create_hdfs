import datetime as dt
import json

def change_parameter_2_string(arecord):
    each_row={}
    for key,values in arecord.iteritems():
        if key=='parameters':
            each_row[key] = json.dumps(values)
        else:
            each_row[key] = values
    if 'traceid' not in arecord:
        each_row['traceid'] = ''
    return each_row
    
def change_to_normal_date(atext):
    try:
        if atext == 'null':
            return ''
        else:
            return dt.datetime.fromtimestamp(float(atext)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print('Could not convert unix date to a normal date {}'.format(e))
        return ' '
    
def check_length_of_incoming_data(df):
    return df.count()

def get_date_string(s):
    try:
        return (dt.datetime.strptime(s, "%Y-%m-%d")).strftime('%Y%m%d')
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)
        return dt.datetime.today().strftime('%Y%m%d')