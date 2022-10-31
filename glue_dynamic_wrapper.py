import boto3
import argparse
from six.moves import configparser
import os
from datetime import date
from deepdiff import DeepDiff


# config prefix a  downloading to my talend /data/  then i will be making the changes to the local copy, uploading
s3_res = boto3.resource('s3')
parser1 = argparse.ArgumentParser(description='Readingconfig file')
parser1.add_argument('--bucket', dest="bucket", help="table_name", required=True)
parser1.add_argument('--sample_config_prefix', dest="config_prefix", help="table_name", required=True)
parser1.add_argument('--source_folder_prefix', dest="folder_prefix", help="table_name", required=True)
parser1.add_argument('--config_dest_prefix', dest="dest_prefix", help="table_name", required=True)
parser1.add_argument('--threshold', dest="threshold", help="table_name", required=True)
parser1.add_argument('--g1xworkernodes', dest="g1xworkernodes", help="table_name", required=True)
parser1.add_argument('--g2xworkernodes', dest="g2xworkernodes", help="table_name", required=True)
parser1.add_argument('--configfile', dest="configfile", help="table_name", required=True)

#parse all the arguments provided and copy it to local variable
parser1_config = parser1.parse_args()
bucket = parser1_config.bucket
config_prefix = parser1_config.config_prefix
folder_prefix = parser1_config.folder_prefix
dest_prefix = parser1_config.dest_prefix
threshold = int(parser1_config.threshold)
g1xworkernodes=parser1_config.g1xworkernodes
g2xworkernodes=parser1_config.g2xworkernodes
configfile = parser1_config.configfile

def adhoc_path_generation(bucket,prefix):
    #Responsible for getting the config file from the given prefix
    bucket_name = s3_res.Bucket(bucket)
    keys = ["s3://"+bucket+"/"+o.key for o in bucket_name.objects.filter(Prefix=prefix)]
    print(keys)
    return keys

def get_folder_size(bucket, prefix):
    #responsible for getting the size of the folder.  Return in bytes.
    total_size = 0
    for obj in boto3.resource('s3').Bucket(bucket).objects.filter(Prefix=prefix):
        # print(obj.size)
        total_size += obj.size
    return total_size

keys = adhoc_path_generation(bucket,config_prefix)

#downloading the config locally and create a copy with the local changes
for key in keys:
    split_string = key.split('/')
    print(split_string)
    if (split_string[-1] == configfile):
        dest_filename = split_string[-1]
        s3_res.meta.client.download_file(bucket, config_prefix + split_string[-1],
                                         '/data/dynamic_config_wrapper/' + split_string[-1])
        parser = configparser.ConfigParser()
        parser.optionxform = str
        file = parser.read("/data/dynamic_config_wrapper/" + split_string[-1])

file_size = get_folder_size(bucket,folder_prefix)
print("In bytes"+str(file_size))

#computing the file size in GB.
file_size_in_gb = file_size/1024/1024/1024
print("In GB"+str(file_size_in_gb))
if(file_size_in_gb<threshold):
    parser.set("job-dpu", "WorkerType",
               "G.1X")
    parser.set("job-dpu", "NumberOfWorkers",
               g1xworkernodes)

elif(file_size_in_gb>threshold):
    parser.set("job-dpu", "WorkerType",
               "G.2X")
    parser.set("job-dpu", "NumberOfWorkers",
               g2xworkernodes)

print(dest_filename)
split_string_arr = dest_filename.split('.')

upload_file_name = split_string_arr[0] +"_upd.conf"
print(upload_file_name)
with open('/data/dynamic_config_wrapper/'+split_string_arr[0]+upload_file_name, 'w') as configfile:
    parser.write(configfile)

print(dest_filename)
split_string_arr = dest_filename.split('.')
old_config = configparser.ConfigParser()
new_config = configparser.ConfigParser()

#reading both old config and new config and do the comparison
old_config.read("/data/dynamic_config_wrapper/" + dest_filename)
new_config.read("/data/dynamic_config_wrapper/"+ upload_file_name)

old_dict = old_config._sections
new_dict = new_config._sections
# print(old_dict)
# print(new_dict)
if old_dict==new_dict:
    pass
else:
    s3_res.meta.client.upload_file("/data/dynamic_config_wrapper/"+ upload_file_name, bucket,
                                   dest_prefix + dest_filename)
    today = date.today()

    dest_filename_fors3=split_string_arr[0]+"_"+str(today)+".conf"
    s3_res.meta.client.upload_file("/data/dynamic_config_wrapper/" + dest_filename, bucket,
                                   dest_prefix + dest_filename_fors3)

s3_res.meta.client.upload_file('/data/dynamic_config_wrapper/'+split_string_arr[0]+'_upd.conf', bucket,
                                           dest_prefix + dest_filename)

s3_conf_file_location = "s3://" + bucket + "/" + dest_prefix + dest_filename

#executing glue_spark_deploy wrapper script.
cmd = """python3 glue_spark_deploy.py {config_file}""".format(
    config_file=s3_conf_file_location)
print(cmd)
os.system(cmd)
os.remove('/data/dynamic_config_wrapper/' + upload_file_name)
os.remove('/data/dynamic_config_wrapper/' + dest_filename)




