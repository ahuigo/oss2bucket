import oss2
import os
from oss2.api import Bucket
from pathlib import Path
from multiprocessing import Pool
from datetime import datetime
import functools

"""
Example:
bucket = Oss2Bucket(oss['AccessKeyId'], oss['AccessKeySecret'], oss['EndPoint'], bucket_name)
bucket.download_from_cloud(remote_dir, local_dir)
"""
class Oss2Bucket(Bucket):
    def __init__(self, AccessKeyId, AccessKeySecret, EndPoint, bucket_name):
        auth = oss2.Auth(AccessKeyId, AccessKeySecret)
        super().__init__(auth, EndPoint, bucket_name)
        self.debug = True

    def logDebug(self, debug = True):
        self.debug = debug


    """
    oss 文件夹上传
    """
    def upload_to_cloud(bucket, local_dir, remote_dir, pool_num=30, chunksize=100):
        if bucket.debug:
            print("Start upload:", local_dir, " at", datetime.now())
        local_dir = Path(local_dir)
        remote_dir = Path(remote_dir)
        try:
            filelist = bucket.get_local_filelist(local_dir)
            pool = Pool(pool_num)
            pool.map(functools.partial(bucket.upload_file, local_dir, remote_dir), filelist, chunksize=chunksize)
        except Exception as err:
            raise Exception("Upload {} to oss failed remote_dir: {}, local_dir: {}".format(err, remote_dir, local_dir))
        if bucket.debug:
            print("End upload:", local_dir, " at", datetime.now())


    """
    oss 文件夹下载(默认30进程)
    """
    def download_from_cloud(bucket, remote_dir, local_dir, pool_num=30, chunksize=100):
        if bucket.debug:
            print("Start download:", remote_dir, " at", datetime.now())
        try:
            filelist = bucket.get_cloud_filelist(remote_dir)
            pool = Pool(pool_num)
            pool.map(functools.partial(bucket.download_file, remote_dir, local_dir), filelist, chunksize=chunksize)
            
        except Exception as err:
            raise Exception("Download {} to oss failed {}".format(os.path.basename(remote_dir), err))
        if bucket.debug:
            print("End download:", remote_dir, " at", datetime.now())

    """
    get local filelist
    """
    def get_local_filelist(self, local_dir):
        for local_file in local_dir.glob('**/*.*'):
            yield local_file

    """
    upload_file
    """
    def upload_file(bucket, local_dir, remote_dir, local_file):
        remote_file = remote_dir/local_file.relative_to(local_dir)
        bucket.put_object_from_file(str(remote_file), str(local_file))

    """
    get_cloud_filelist
    """
    def get_cloud_filelist(bucket, remote_dir):
        remote_dir = remote_dir.strip('/')+'/'
        read_dir = True
        next_marker = False
        while read_dir:
            bl = bucket.list_objects(remote_dir, marker=next_marker, max_keys=1000)
            next_marker = bl.next_marker
            for remote_file in bl.object_list:
                if not remote_file.key.endswith('/'):
                    yield remote_file.key
            read_dir = next_marker

    """
    download file
    """
    def download_file(bucket, remote_dir, local_dir, remote_file):
            local_file = Path(local_dir)/Path(remote_file).relative_to(remote_dir)
            if not local_file.parent.exists():
                os.makedirs(str(local_file.parent), exist_ok=True)
            bucket.get_object_to_file(str(remote_file), str(local_file))

if __name__ == '__main__':
    import argparse
    import time
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", '--src', default="2019-03-08-16-13-11_uuid9876543210_longdeguangchang-map-construct_rawdata", help="package名")
    parser.add_argument("-d", '--dst',default="download",  help="下载路径")
    parser.add_argument("-e", '--env', default="dev", choices=['dev','staging','production'], help='生产环境')
    cli_args = parser.parse_args()

    remote_dir = 'origin-data/'+cli_args.src
    local_dir = cli_args.dst
    print('Download from :', remote_dir)


    from ossconfig import ossconfig
    bucket_name = ossconfig['MvpBucket'][cli_args.env]
    bucket = Oss2Bucket(ossconfig['AccessKeyId'], ossconfig['AccessKeySecret'], ossconfig['EndPoint'], bucket_name)
    bucket.download_from_cloud(remote_dir, local_dir)
    #bucket.upload_to_cloud('test', 'test2', 2, 2)
    print("Finished!")
    print("elapsed time:%f s" % (time.time()-start_time))
