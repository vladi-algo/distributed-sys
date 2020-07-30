import os
stream = os.popen('docker exec etcd-gcr-v3.4.10 /bin/sh -c "/usr/local/bin/etcdctl endpoint health"')
output = stream.read()
print(output)