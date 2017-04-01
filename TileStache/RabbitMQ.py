""" Caches tiles to Amazon S3.

Requires boto (2.0+):
  http://pypi.python.org/pypi/boto

Example configuration:

  "cache": {
    "name": "S3",
    "bucket": "<bucket name>",
    "access": "<access key>",
    "secret": "<secret key>"
  }

S3 cache parameters:

  bucket
    Required bucket name for S3. If it doesn't exist, it will be created.

  access
    Optional access key ID for your S3 account.

  secret
    Optional secret access key for your S3 account.

  use_locks
    Optional boolean flag for whether to use the locking feature on S3.
    True by default. A good reason to set this to false would be the
    additional price and time required for each lock set in S3.
    
  path
    Optional path under bucket to use as the cache dir. ex. 'cache' will 
    put tiles under <bucket>/cache/

Access and secret keys are under "Security Credentials" at your AWS account page:
  http://aws.amazon.com/account/
  
When access or secret are not provided, the environment variables
AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY will be used
    http://docs.pythonboto.org/en/latest/s3_tut.html#creating-a-connection
"""

try:
    import pika
    from msgpack import packb
except ImportError:
    # at least we can build the documentation
    pass

class Cache:
    """
    """
    def __init__(self, queue=None, access=None):
        credentials = pika.PlainCredentials('mblissett', 'mblissett')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('mq-vh.gbif.org', 5672, '/users/mblissett', credentials))
        self.channel = self.connection.channel()

        self.queue = queue
        self.channel.queue_declare(queue=self.queue)

        self.i = 0

    def lock(self, layer, coord, format):
        return
        
    def unlock(self, layer, coord, format):
        return
        
    def remove(self, layer, coord, format):
        return
        
    def read(self, layer, coord, format):
        return
        
    def save(self, body, layer, coord, format):
        """ Save a cached tile.
        """
        self.i = self.i + 1
        if (self.i % 100 == 0):
            print('ToMQ', self.i, coord)

        msg = packb({
            "layer": layer.name(),
            "zoom": coord.zoom,
            "column": coord.column,
            "row": coord.row,
            "tile": body
        })

        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue,
                                   body=msg)
