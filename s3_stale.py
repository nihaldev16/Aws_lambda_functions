import boto3
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

# Initialize S3 client
s3_client = boto3.client('s3')

# Define the threshold for last accessed/modified time (e.g., 180 days)
THRESHOLD_DAYS = 180

def lambda_handler(event, context):
    try:
        # Get the current time
        current_time = datetime.now(timezone.utc)
        
        # List all S3 buckets
        response = s3_client.list_buckets()
        buckets = response['Buckets']
        
        # Iterate over each bucket and check last modified time
        for bucket in buckets:
            bucket_name = bucket['Name']
            
            # Get the last modified object in the bucket
            last_modified = get_last_modified_object(bucket_name)
            
            if last_modified:
                # Calculate the age of the bucket since the last modification
                age = (current_time - last_modified).days
                
                # If the age exceeds the threshold, delete the bucket
                if age > THRESHOLD_DAYS:
                    print(f"Bucket {bucket_name} is stale (last modified {age} days ago). Deleting...")
                    delete_bucket(bucket_name)
                else:
                    print(f"Bucket {bucket_name} is still active (last modified {age} days ago).")
            else:
                print(f"Bucket {bucket_name} is empty or has no objects.")
                
    except ClientError as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": str(e)}

    return {"statusCode": 200, "body": "Stale buckets deleted successfully"}

def get_last_modified_object(bucket_name):
    """Get the last modified object in an S3 bucket."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' not in response:
            return None
        
        # Get the most recent object by comparing LastModified timestamps
        objects = response['Contents']
        last_modified_obj = max(objects, key=lambda obj: obj['LastModified'])
        
        return last_modified_obj['LastModified']
    
    except ClientError as e:
        print(f"Error fetching objects for bucket {bucket_name}: {e}")
        return None

def delete_bucket(bucket_name):
    """Delete an S3 bucket and its contents."""
    try:
        # First delete all objects in the bucket
        delete_all_objects(bucket_name)
        
        # Then delete the empty bucket itself
        s3_client.delete_bucket(Bucket=bucket_name)
        
        print(f"Bucket {bucket_name} deleted successfully.")
    
    except ClientError as e:
        print(f"Error deleting bucket {bucket_name}: {e}")

def delete_all_objects(bucket_name):
    """Delete all objects in an S3 bucket."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            # Delete all objects in one call using delete_objects
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            
            print(f"All objects deleted from bucket {bucket_name}.")
    
    except ClientError as e:
        print(f"Error deleting objects from bucket {bucket_name}: {e}")
