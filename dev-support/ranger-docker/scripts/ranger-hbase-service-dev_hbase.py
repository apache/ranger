from apache_ranger.model.ranger_service     import RangerService
from apache_ranger.client.ranger_client     import RangerClient

ranger_client = RangerClient('http://ranger:6080', 'admin', 'rangerR0cks!')

service = RangerService(name='dev_hbase', type='hbase', configs={'username':'hbase', 'password':'hbase', 'hadoop.security.authentication': 'simple', 'hbase.security.authentication': 'simple', 'hadoop.security.authorization': 'true', 'hbase.zookeeper.property.clientPort': '16181', 'hbase.zookeeper.quorum': 'ranger-hbase', 'zookeeper.znode.parent': '/hbase'})

ranger_client.create_service(service)
