import etcd3
import sys
import time
from threading import Event

LEADER_KEY = "/key/leader"
# if leader doesn't refresh the lease within TTL period - the new leader election process starts.
LEASE_TTL_SEC = 10

'''
Create a new leader entry in etcd with the input lease.
If a lease is expired (in case the leader didn't refresh it) - the related key will be expired and deleted.
If leader key still exists - the function exit (the leader is still alive), otherwise a new entry (leader) 
is created with the attached lease.

'''


def set_leader(client, server, lease, key):
    # attempt to write leader candidate to etcd in transaction
    print("attempt to save leader in etcd")
    status, response = client.transaction(
        compare=[
            client.transactions.version(key) == 0
        ],
        success=[
            client.transactions.put(key, server, lease)
        ],
        failure=[],
    )
    return status


'''
Start leader election:
1. Create a lease with input TTL
2. Call to set_leader function - to set a new leader
;:param client: etcd3 client
;:param server: server name, the server name attempting to become a leader
'''


def elect_leader(client, server):
    print("start leader election....")
    try:
        lease = client.lease(LEASE_TTL_SEC)
        status = set_leader(client, server, lease, LEADER_KEY)
    except Exception:
        status = False
        print("elect leader for server :" + server + " status: " + str(status))
    return status, lease


'''
This function is executed only by a leader.
The leader must take care of refreshing the lease, otherwise the lease will be expired and the leader's key is deleted
and new leader election will start.
In case of exception the lease is revoked and new leader election starts

'''


def leader_work(lease):
    print('I''M A LEADER!')
    try:
        while True:
            lease.refresh()
            # do some leader work here
            print('The leader is doing work...')
            time.sleep(0.5)
    except Exception:
        print("Exception in leader happened")
        return
    except KeyboardInterrupt:
        return
    finally:
        lease.revoke()


'''
This function is executed only by the followers.
All followers are in busy waiting for leader key deletion due to lease TTL or revoking by the leader itself
'''


def follower_work(client):
    election_start_event = Event()

    def callback_for_start_election(watch_response):
        for event in watch_response.events:
            if isinstance(event, etcd3.events.DeleteEvent):
                print("Start new leader election!!")
                election_start_event.set()

    callback_id = client.add_watch_callback(LEADER_KEY, callback_for_start_election)

    try:
        while not election_start_event.is_set():
            time.sleep(1)

        # the leader key is deleted, start the new election
    except Exception:
        print("Exception in follower happened")
        return
    except KeyboardInterrupt:
        return
    finally:
        client.cancel_watch(callback_id)


def main_leader_election(server):
    etcd_client = etcd3.client(timeout=3)

    while True:
        # elect leader:
        leader, lease = elect_leader(etcd_client, server)

        if leader:
            leader_work(lease)
            time.sleep(1)

        else:  # follower
            print("server : " + server + " not a leader")
            follower_work(etcd_client)


if __name__ == '__main__':
    server = sys.argv[1]
    main_leader_election(server)
