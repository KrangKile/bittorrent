import random
import logging
from collections import defaultdict

from messages import Upload, Request
from util import even_split
from peer import Peer
from krankilestd import KrankileStd

class KrankileTyrant(KrankileStd):
    def post_init(self):
        self.alpha = 0.20
        self.gamma = 0.10
        self.r = 3

        # Make an initial estimate for expected download performance d_{ij}.
        # Since we cannot use the have-messages for estimation of the download capacity we can
        # get from a given peer, we instead use the knowledge of the range of different upload
        # bandwidths that exists, and divide that by 4 on an assumption of that is what the
        # reference client is using.
        self.downloads = defaultdict(lambda: random.randint(self.conf.min_up_bw, self.conf.max_up_bw) / 4)
        
        # Make an initial for upload required for reciprocation u_{ij}
        # In the make use of the knowledge of the distribution of bandwidths and the assumption that the
        # other clients in the neighborhood are reference clients with 4 slots. This yields a higher bw per
        # slot which should increase likelihood of reciprocation. 
        self.upload_bws = defaultdict(lambda: max(random.randint(self.conf.min_up_bw, self.conf.max_up_bw) / 3, 1))

        print("upload_bws", self.upload_bws[1])

    def get_ratio(self, id_):
        return float(self.downloads[id_]) / self.upload_bws[id_]

    def sort_func(self, id1, id2):
        return int(round((self.get_ratio(id2) - self.get_ratio(id1)) * 1000))

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        # To update the values for alpha, gamma and r before we do uploading in this round
        # should be the same as updating the values after a round.
        if history.current_round() != 0:
            # We have completed 1 round and we have history
            unchoked_ids = set(download.from_id for download in history.downloads[-1])
            # Step 1
            # Find the set of peers in the neighborhood that has not unchoked this agent
            choked_ids = set(peer.id for peer in peers).difference(unchoked_ids)
            for peer_id in choked_ids:
                self.upload_bws[peer_id] = max(self.upload_bws[peer_id] * (1 + self.alpha), 1)

            # Step 2
            # A map of peers that unchoked this agent last period and how much bw we received   
            unchoked = {download.from_id: download.blocks for download in history.downloads[-1]}
            for peer_id, rate in unchoked.items():
                # Update the estimated download rate from a peer with the actual, observed value
                self.downloads[peer_id] = rate

            # Step 3
            # Initialize the set for holding the set of peers that uploaded to this agent for the last r periods
            unchoked_r_last = set(unchoked)
            for downloads in history.downloads[-self.r:-1]:
                unchoked_r_last = unchoked_r_last.intersection(set(download.from_id for download in downloads))
            
            # Slowly decrease the bw we give to these peers while they hopefully still reciprocates
            for peer_id in unchoked_r_last:
                self.upload_bws[peer_id] = min(self.upload_bws[peer_id] * (1 - self.gamma), 1)

        # In case there were no requests, just terminate
        if len(requests) == 0:
            return []

        # Find the peers that have sent requests to this agent
        requester_ids = list(set(request.requester_id for request in requests))
        sorted_ids = sorted(requester_ids, self.sort_func)

        # Choose peers to unchoke until the maximum upload capacity is reached
        bandwidth_used = 0
        chosen = []
        for id_ in sorted_ids:

            if self.up_bw <= bandwidth_used + self.upload_bws[id_]:
                break

            chosen.append(id_)
            bandwidth_used += self.upload_bws[id_]

        # create actual uploads out of the list of peer ids
        uploads = [Upload(self.id, peer_id, self.upload_bws[peer_id]) for peer_id in chosen]
        
        return uploads
