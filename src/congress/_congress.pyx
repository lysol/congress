import math

def bucket_leaf(distance, bucket):
    return distance > math.pow(2, bucket) and \
        distance < math.pow(2, bucket + 1)

def bucket_sort(distance, bucket):
    return math.fabs(distance - math.pow(2, bucket))
