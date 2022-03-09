
def create_bucket(constant, specific_date):

    bucket = constant.bucket + '/cutoff_date={}/'.format(specific_date) #'y'+

    return bucket
    