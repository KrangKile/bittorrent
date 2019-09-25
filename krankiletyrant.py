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
        self.downloads = dict()
        
        # Start with a value for u_{ij} that equals the reference client's
        self.uploads = defaultdict(lambda: even_split(self.up_bw, 4)[1])

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """

        round_num = history.current_round()
        logging.debug("%s again.  It's round %d." % (self.id, round_num))
        # One could look at other stuff in the history too here.
        # For example, history.downloads[round-1] (if round != 0, of course)
        # has a list of Download objects for each Download to this peer in
        # the previous round.

        if len(requests) == 0:
            logging.debug("No one wants my pieces!")
            return []

        logging.debug("Still here: uploading to a random peer")
        # change my internal state for no reason
        self.dummy_state["cake"] = "pie"

        request = random.choice(requests)
        chosen = [request.requester_id]
        # Evenly "split" my upload bandwidth among the one chosen requester
        bws = even_split(self.up_bw, len(chosen))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw)
                   for (peer_id, bw) in zip(chosen, bws)]
            
        return uploads
