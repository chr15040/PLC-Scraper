import os
import boto3
AWS_S3_BUCKET_NAME= "ist-webplatform-archie-dev"
class S3Bucket:

    def __init__(self):
        self.bucket_name = AWS_S3_BUCKET_NAME

    def dl_data(self, faiss=False, scrape_data=False):
        s3 = boto3.resource('s3')

        bucket = s3.Bucket(self.bucket_name)

        for s3_object in bucket.objects.all():
            path, filename = os.path.split(s3_object.key)
            if "Latest" in path and not os.path.exists(path):
                os.makedirs(path)
            if faiss and "Latest" in path and not filename.endswith('.csv') and not filename.endswith('.txt'):
                bucket.download_file(s3_object.key, s3_object.key)
            elif scrape_data and "Latest" in path and filename.endswith('.csv'):
                bucket.download_file(s3_object.key, s3_object.key)

    def upload_file(self, file_name, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3 = boto3.client('s3')
        try:
            response = s3.upload_file(file_name, self.bucket_name, object_name)
        except Exception as e:
            print(f'Error uploading {file_name} to {self.bucket_name}: {str(e)}')
            return False
        print(f'{file_name} successfully uploaded to S3 bucket: {self.bucket_name}')
        return True