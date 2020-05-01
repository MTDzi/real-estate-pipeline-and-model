


def get_bucket(s3_url):
    return s3_url.split('/')[2]


def get_subbucket(s3_url):
    return s3_url.split('/')[3]