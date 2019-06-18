import argparse
import os
import oss2
import threading


class RemoveOSS:
    def __init__ (self, access_key, secret_key, endpoint, bucket, max_threads, retries):
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint = endpoint
        self.bucket = self._auth(bucket)
        self.max_threads = max_threads
        self.retries = retries


    def _auth(self, bucket):
        auth = oss2.Auth(self.access_key, self.secret_key)
        oss_bucket = oss2.Bucket(auth, self.endpoint, bucket)
        return oss_bucket


    def _bulk_delete(self, objects):
        result = self.bucket.batch_delete_objects(objects)
        print("{0}{1}".format('Deleted: ', '\nDeleted: '.join(result.deleted_keys)))


    def _delete(self, object):
        result = self.bucket.delete_object(object)
        return result


    def _threaded_bulk_delete(self, object_list):
        jobs = []
        for obj in object_list:
            thread = threading.Thread(target=self._bulk_delete(obj))
            jobs.append(thread)

        for job in jobs:
            job.start()

        for job in jobs:
            job.join()


    def _threaded_delete(self, object_list):
        jobs = []
        for obj in object_list:
            thread = threading.Thread(target=self._delete(obj))
            jobs.append(thread)

        for job in jobs:
            job.start()

        for job in jobs:
            job.join()


    def get_bucket(self):
        return self.bucket


    def delete_oss_objects(self):
        '''
        Delete all objects and folders (excluding fragments) from OSS bucket.
        To speed up the deletion process for large buckets, the removal of files is multi-threaded. 

        TODO: ability specify a list of buckets for removal
        '''
        count = 0
        object_list = []
        temp_list = []
        folders = []

        # compile list of objects
        for obj in oss2.ObjectIterator(self.bucket):
            for attempts in range(self.retries):
                try:
                    if not obj.key.endswith("/"):
                        temp_list.append(obj.key)
                        # bulk delete in batch of 1000
                        if count % 1000 == 0:
                            object_list.append(temp_list)
                            temp_list = []
                    else:
                        folders.append(obj.key)

                    if len(object_list) == self.max_threads:
                        self._threaded_bulk_delete(object_list)

                        object_list = []

                    count += 1
                except oss2.exceptions.RequestError:
                    pass
                # success within max number of attempts, continue
                else:
                    break
            # we failed all attempts
            else:
                print('Connection error occurred. Exceeded max number of retries: {}'.format(self.retries))

        # clean up remaining obj
        self._threaded_bulk_delete(object_list)

        # round 2: delete remaining folders
        for obj in oss2.ObjectIterator(self.bucket):
            folders.append(obj.key)

        if folders:
            self._bulk_delete(folders)

        else:
            print('No remaining folders/objects to be deleted.')


if __name__ == "__main__":
    # argparse
    parser = argparse.ArgumentParser(description="Aliyun credentials and bucket name")
    parser.add_argument('-a', '--access_key', help="Aliyun Access Key")
    parser.add_argument('-s', '--secret_key', help="Aliyun Secret Key")
    parser.add_argument('-o', '--oss_endpoint', help="OSS Endpoint")
    parser.add_argument('-b', '--bucket', required=True, help="Bucket name")
    parser.add_argument('-t', '--max_threads', default=4, help="Max number of threads")
    parser.add_argument('-r', '--retries', default=5, help="Max number of retries")
    args = parser.parse_args()

    args.access_key = args.access_key or os.environ['ALICLOUD_ACCESS_KEY'] # Access Key Id
    args.secret_key = args.secret_key or os.environ['ALICLOUD_SECRET_KEY'] # Access Key Secret
    args.oss_endpoint = args.oss_endpoint or 'http://oss-cn-shanghai.aliyuncs.com'
    
    oss = RemoveOSS(args.access_key, args.secret_key, args.oss_endpoint, args.bucket, args.max_threads, args.retries)
    
    oss.delete_oss_objects()

